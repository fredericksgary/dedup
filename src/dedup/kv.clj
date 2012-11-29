(ns dedup.kv
  (:refer-clojure :exclude [get]))

(def store (atom {}))

(defn get
  [k]
  (@store k))

(defn put
  [k v]
  (swap! store assoc k v))