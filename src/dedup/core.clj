(ns dedup.core
  (:refer-clojure :exclude [hash])
  (:require [dedup.sha1 :refer [sha1]]
            [clojure.string :as s]
            [clojure.walk :as wk]
            [dedup.kv :as kv]
            [cheshire.core :as json])
  (:gen-class))

(deftype Reference [sha])

(defonce true-hash (sha1 "boolean true"))
(defonce false-hash (sha1 "boolean false"))
(defonce nil-hash (sha1 "nil"))

(defprotocol Hashable
  (hash [this]))

(extend-protocol Hashable
  String
  (hash [this] (sha1 (str "string " this)))
  Long
  (hash [this] (sha1 (str "long " this)))
  clojure.lang.APersistentMap
  (hash [this]
    (let [sorted-keys (sort (keys this))]
      (sha1
       (format "map %s %s"
               (hash (vec sorted-keys))
               (hash (vec (map this sorted-keys)))))))
  clojure.lang.APersistentVector
  (hash [this]
    (->> this
         (map hash)
         (cons "vector")
         (s/join " ")
         sha1))
  Boolean
  (hash [this]
    (if this true-hash false-hash))
  nil
  (hash [this] nil-hash)
  Reference
  (hash [this] (.sha this)))

(defmethod print-method Reference
  [ref pw]
  (.append pw "#dedup.core/Reference ")
  (binding [*out* pw] (pr (.sha ref))))

(defn store
  [ob]
  (wk/postwalk (fn [x] (let [h (hash x)]
                         (kv/put h x)
                         (->Reference h)))
               ob))

(defn materialize
  [ref]
  (wk/prewalk (fn [ref] (kv/get (.sha ref))) ref))

(defn standard-types
  [ob]
  (wk/postwalk (fn [x] (if (instance? Integer x) (long x) x)) ob))

(defn -main
  "Reads a bunch of JSON objects from STDIN and prints the dedup'd data store."
  []
  (doseq [line (line-seq (java.io.BufferedReader. *in*))]
    (store (standard-types (json/parse-string line))))
  (prn @kv/store))