(ns event-sourcing.core
  (:import [org.apache.kafka.streams StreamsConfig KafkaStreams StreamsBuilder KeyValue]
           [org.apache.kafka.streams.state QueryableStoreTypes]
           [org.apache.kafka.streams.kstream ValueMapper Reducer JoinWindows ValueTransformer Transformer
            TimeWindows]

           [org.apache.kafka.clients.consumer ConsumerConfig]
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           [java.time Duration])
  (:require [jackdaw.streams :as j]
            [jackdaw.client :as jc]
            [jackdaw.admin :as ja]
            [jackdaw.serdes.edn :as jse]
            [jackdaw.serdes :as js]
            [jackdaw.serdes.json :as jj]
            [event-sourcing.flight-time-analytics :as flight-time-analytics]
            [event-sourcing.passenger-counting :as passenger-counting]
            [event-sourcing.delay-finder :as delay-finder]
            [event-sourcing.decisions :as decisions]
            [event-sourcing.query :as query]
            [event-sourcing.transducer :as transducer]
            [event-sourcing.utils :refer [topic-config]]
            [clojure.set :as set]
            [clojure.string :as str]))


(def log-config {"bootstrap.servers"
                ;; data
                ;"10.1.21.44:9092,10.1.6.141:9092,10.1.28.103:9092"
                                         ;log
                                         "10.1.143.165:9092,10.1.139.102:9092,10.1.141.233:9092"
                 StreamsConfig/APPLICATION_ID_CONFIG "a-log"
                 StreamsConfig/COMMIT_INTERVAL_MS_CONFIG 3000
                 ; ConsumerConfig/AUTO_OFFSET_RESET_CONFIG "latest"
                 ConsumerConfig/AUTO_OFFSET_RESET_CONFIG "earliest"
                 "acks"              "all"
                 "retries"           "0"
                 "cache.max.bytes.buffering" "10485760"})


(def app-config {"bootstrap.servers"  "10.3.18.110:9092"  ;; "localhost:9092"
                 StreamsConfig/APPLICATION_ID_CONFIG "a-flight-app-8"
                 StreamsConfig/COMMIT_INTERVAL_MS_CONFIG 500
                 ConsumerConfig/AUTO_OFFSET_RESET_CONFIG "latest"
                 "acks"              "all"
                 "retries"           "0"
                 "cache.max.bytes.buffering" "0"})

(defn confirm-topic! [topic-name]
  (let [topic          (topic-config topic-name)
        admin-client   (ja/->AdminClient log-config)
        topic-existed? (ja/topic-exists? admin-client topic)]
    (println "topic-existed? " topic-name ":" topic-existed?)
                                        ;(when topic-existed?
                                        ;  (ja/delete-topics! admin-client [topic]))
                                        ; (ja/create-topics! admin-client [topic])
    (when (not topic-existed?)
      (ja/create-topics! admin-client [topic]))
    ))

(defn produce-one
  ([topic k v ]
   (with-open [producer (jc/producer log-config (topic-config topic))]
     @(jc/produce! producer (topic-config topic) k v))))

(defonce stream-app (atom nil))
(defonce continue-monitoring? (atom true))

(defn start-topology
  ([topology-preparer!]
   (start-topology topology-preparer! log-config))
  ([topology-prepare! log-config]
   (let [streams-builder (j/streams-builder)
         _ (topology-prepare! streams-builder)
         _ (println (java.util.Date.))
         _ (println (-> streams-builder j/streams-builder* .build .describe .toString))
         kafka-streams (j/kafka-streams streams-builder log-config)]
     (reset! stream-app kafka-streams)
     (j/start kafka-streams))))

(defn shutdown []
  (when @stream-app
    (j/close @stream-app))
  (when @continue-monitoring?
    (reset! continue-monitoring? false)))


(defn monitor-topics
  ([topics]
   (reset! continue-monitoring? true)
   (doall (map confirm-topic! topics))
   (future
     (with-open [subscription (jc/subscribed-consumer (assoc log-config "group.id" "monitorx")
                                                      (map topic-config topics))]
       (loop [results (jc/poll subscription 200)]
         (doseq [{:keys [topic-name key value]} results]
           (println   "Topic:" topic-name
                    "\n  Key:" key
                    "\nValue:" (str/replace (with-out-str (clojure.pprint/pprint value)) #"\n" "\n       ")))
         (if @continue-monitoring?
           (recur (jc/poll subscription 200))
           nil))))))


;; Example events
(comment
  [{:flight "UA1496"}
   {:event-type :passenger-boarded
    :who "Leslie Nielsen"
    :time #inst "2019-03-16T00:00:00.000-00:00"
    :flight "UA1496"}]

  [{:flight "UA1496"}
   {:event-type :departed
    :time #inst "2019-03-16T00:00:00.000-00:00"
    :flight "UA1496"
    :scheduled-departure #inst "2019-03-15T00:00:00.000-00:00"}]

  [{:flight "UA1496"}
   {:event-type :arrived
    :time #inst "2019-03-17T04:00:00.000-00:00"
    :flight "UA1496"}]

  [{:flight "UA1496"}
   {:event-type :passenger-departed
    :who "Leslie Nielsen"
    :time #inst "2019-03-17T05:00:00.000-00:00"
    :flight "UA1496"}])

(defn log-count-topology [in-topic out-topic]
    (fn [builder]
      (let [window       (Duration/ofSeconds 10)
            grace        (Duration/ofSeconds 1)
            time-windows (TimeWindows/ofSizeAndGrace window grace)
            ]
        (-> builder
            (j/kstream (topic-config in-topic))
                                        ;(j/filter (fn [[k v]]
                                        ;            (and
                                        ;             (= (:event-type v) :departed))))
           (j/map (fn [[_ v]] [(subs (:time_iso8601 v) 0 16)
                               #_(:http_uni_openid_hash v)
                               (-> v
                                   :request
                                   (#(re-find #"\w+ [/\-\w]*" %))
                                   (clojure.string/replace #"\w{10,}" "Num")
                                   )]))
           ;(j/group-by-key)
           ;(j/window-by-time time-windows)
           ;                            ;(j/aggregate (constantly {})
           ;                            ;             (fn [acc [_k request]]
           ;                            ;               (println request)
           ;                            ;               (update acc request (fnil inc 0))
           ;                            ;               ))
           ;(j/aggregate (constantly 0)
           ;             (fn [acc _] (inc acc))
           ;             {:topic-name "agg-of-lu"
           ;              :partition-count 1
           ;              :replication-factor 1
           ;              :key-serde (js/string-serde)
           ;              :value-serde (jse/serde)}
           ;             )
           ;(j/to-kstream)
           (j/to (topic-config out-topic))
           ;(j/process! (fn [_ k v]
           ;              (println [k, v]))
           ;            [])
                       ))
      builder))

(comment
  (do (shutdown)
      ; (start-topology (log-count-topology "prod_ucontent_user_struct_progress" "a-flight-status"))
                                        (start-topology (log-count-topology "ucontent" "a-flight-status"))
                                        ; (monitor-topics ["nginx-log-itest"])
      )

  (topic-config "k12_nginx_log")
  (monitor-topics ["nginx-log-itest"])

  )

;; EXAMPLE 1: Finds delayed flights from flight-events, writes to flight-status
(comment
  (do (shutdown)
      (start-topology (delay-finder/find-delays-topology "a-flight-events" "a-flight-status"))
      (monitor-topics ["a-flight-events" "a-flight-status"]))

  ;; delayed departure
  (produce-one "a-flight-events"
               {:flight "UA1496"}
               {:event-type :departed
                :time #inst "2019-03-16T00:00:00.000-00:00"
                :flight "UA1496"
                :scheduled-departure #inst "2019-03-15T00:00:00.000-00:00"})

  ;; on-time departure
  (produce-one "a-flight-events"
               {:flight "UA1497"}
               {:event-type :departed
                :time #inst "2019-03-16T00:00:00.000-00:00"
                :flight "UA1497"
                :scheduled-departure #inst "2019-03-16T00:00:00.000-00:00"})
  )



;; EXAMPLE 2: How long is a flight in the air?
(comment
  (do (shutdown)
      (start-topology (flight-time-analytics/build-time-joining-topology "a-flight-events" "a-flight-times"))
      (monitor-topics ["a-flight-events" "a-flight-times"]))

  (do
    (produce-one "a-flight-events"
                 {:flight "UA1496"}
                 {:event-type :departed
                  :time #inst "2019-03-16T00:00:00.000-00:00"
                  :flight "UA1496"
                  :scheduled-departure #inst "2019-03-15T00:00:00.000-00:00"})

    (produce-one "a-flight-events"
                 {:flight "UA1496"}
                 {:event-type :arrived
                  :time #inst "2019-03-18T13:00:00.000-00:00"
                  :flight "UA1496"}))

  (do (shutdown)
      (start-topology (flight-time-analytics/build-table-joining-topology
                       "a-flight-events" "a-flight-times" "a-flight-departures"))
      (monitor-topics ["a-flight-events" "a-flight-times"]))

  (produce-one "a-flight-events"
               {:flight "UA1497"}
               {:event-type :departed
                :time #inst "2019-03-17T02:00:00.000-00:00"
                :flight "UA1497"
                :scheduled-departure #inst "2019-03-15T00:00:00.000-00:00"})

  (for [x (range 10)]
    (produce-one "a-flight-events"
                 {:flight "UA1497"}
                 {:event-type :arrived
                  :time #inst "2019-03-17T03:02:00.000-00:00"
                  :flight "UA1497"})
    )

  )


;; EXAMPLE 3: Who is on the plane?
(comment
  (do (shutdown)
      (start-topology (passenger-counting/build-boarded-counting-topology "a-flight-events" "a-passenger"))
      (monitor-topics ["a-flight-events" "a-passenger"]))

  ;; Leslie Nielsen boarded
  (produce-one "a-flight-events"
               {:flight "UA1496"}
               {:event-type :passenger-boarded
                :who "Leslie Nielsen"
                :time #inst "2019-03-16T00:00:00.000-00:00"
                :flight "UA1496"})

  (query/get-passengers @stream-app "UA1496")

  ;; Leslie Nielsen Departed
  (produce-one "a-flight-events"
               {:flight "UA1496"}
               {:event-type :passenger-departed
                :who "Leslie Nielsen"
                :time #inst "2019-03-16T00:00:00.000-00:00"
                :flight "UA1496"})



  (query/get-passengers @stream-app "UA1496")

  ;; Julie Hagerty boarded
  (produce-one "a-flight-events"
               {:flight "UA1496"}
               {:event-type :passenger-boarded
                :who "Julie Hagerty"
                :time #inst "2019-03-16T00:00:00.000-00:00"
                :flight "UA1496"})

  ;; Julie Hagerty departed
  (produce-one "a-flight-events"
               {:flight "UA1496"}
               {:event-type :passenger-departed
                :who "Julie Hagerty"
                :time #inst "2019-03-16T00:00:00.000-00:00"
                :flight "UA1496"})
  )

;; EXAMPLE 4: Count passengers as they board the plane
(comment
  (do (shutdown)
      (start-topology (passenger-counting/build-boarded-decorating-topology
                       "a-flight-events" "a-flight-events-with-passengers"))
      (monitor-topics ["a-flight-events" "a-flight-events-with-passengers"]))

  ;; Robert Hays boarded
  (produce-one "a-flight-events"
               {:flight "UA1496"}
               {:event-type :passenger-boarded
                :who "Robert Hays"
                :time #inst "2019-03-16T00:00:00.000-00:00"
                :flight "UA1496"})

  (do (shutdown)
      (start-topology (passenger-counting/build-boarded-decorating-topology-cleaner "a-flight-events" "a-flight-events-with-passengers"))
      (monitor-topics ["a-flight-events" "a-flight-events-with-passengers"]))

  ;; Julie Hagerty boarded
  (produce-one "a-flight-events"
               {:flight "UA1496"}
               {:event-type :passenger-boarded
                :who "Julie Hagerty"
                :time #inst "2019-03-16T00:00:00.000-00:00"
                :flight "UA1496"})


  (query/get-passengers @stream-app "UA1496")
  )

;; EXAMPLE 5: Are my friends on the plane?
(comment

  (query/get-passengers @stream-app "UA1496")
  (query/friends-onboard? @stream-app "UA1496" #{"Leslie Nielsen" "Julie Hagerty" "Peter Graves"})
  )



;; EXAMPLE 6: Clean the plane when the last passenger departs
(comment

  (do (shutdown)
      (start-topology decisions/build-clean-plane-topology)
      (monitor-topics ["a-flight-events" "a-flight-decisions"]))

  ;; Leslie Nielsen Departed
  (produce-one "a-flight-events"
               {:flight "UA1496"}
               {:event-type :passenger-departed
                :who "Leslie Nielsen"
                :time #inst "2019-03-16T00:00:00.000-00:00"
                :flight "UA1496"})

  ;; Robert Hays Departed
  (produce-one "a-flight-events"
               {:flight "UA1496"}
               {:event-type :passenger-departed
                :who "Robert Hays"
                :time #inst "2019-03-16T00:00:00.000-00:00"
                :flight "UA1496"})

  ;; Julie Hagerty Departed
  (produce-one "a-flight-events"
               {:flight "UA1496"}
               {:event-type :passenger-departed
                :who "Julie Hagerty"
                :time #inst "2019-03-16T00:00:00.000-00:00"
                :flight "UA1496"})


  (query/get-passengers @stream-app "UA1496")


  )

;; EXAMPLE 7: Fixing a bug
(comment
  (do (shutdown)
      (start-topology decisions/build-clean-plane-topology
                      (assoc log-config
                             StreamsConfig/APPLICATION_ID_CONFIG "cleaning-planner-bugfix"
                             ConsumerConfig/AUTO_OFFSET_RESET_CONFIG "earliest"
                             ))
      (monitor-topics ["a-flight-events" "a-flight-decisions"]))
  )

;; EXAMPLE 8: Transducers
#_(comment
  (do (shutdown)
      (start-topology transducer/build-transducer-topology)
        (monitor-topics ["a-flight-events" "transduced-events"]))
  )


(comment
  (shutdown)
  )
