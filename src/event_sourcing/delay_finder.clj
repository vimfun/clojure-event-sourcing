(ns event-sourcing.delay-finder
  (:require [event-sourcing.utils :refer [topic-config]]
            [jackdaw.streams :as j]))

(defn find-delays-topology [in-topic out-topic] ; ["flight-events" "flight-status"]
  (fn [builder]
    (-> builder
        (j/kstream (topic-config in-topic))
        (j/filter (fn [[k v]]
                    (and
                     (= (:event-type v) :departed))))
        (j/map (fn [[k v]]
                 (if (.after (:time v) (:scheduled-departure v))
                   [k {:status :left-late}]
                   [k {:status :left-on-time}])))
        (j/to (topic-config out-topic)))
    builder))
