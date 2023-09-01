(ns event-sourcing.utils
  (:require [jackdaw.serdes.edn :as jse]
            [jackdaw.serdes :as js]
            [jackdaw.serdes.json :as jj]))

(defn topic-config [name]
  {:topic-name name
   :partition-count 1
   :replication-factor 1
   :key-serde (js/string-serde)
   :value-serde (jj/serde)})
