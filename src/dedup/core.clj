(ns dedup.core
  (:refer-clojure :exclude [hash])
  (:require [dedup.sha1 :refer [sha1]]
            [clojure.string :as s]
            [clojure.walk :as wk]
            [dedup.kv :as kv]
            [cheshire.core :as json])
  (:gen-class))

(deftype Reference [sha])

(def true-hash (sha1 "boolean true"))
(def false-hash (sha1 "boolean false"))
(def nil-hash (sha1 "nil"))

(defprotocol Hashable
  (hash [this]))

(extend-protocol Hashable
  String
  (hash [this] (sha1 (str "string " this)))
  Long
  (hash [this] (sha1 (str "long " this)))
  Double
  (hash [this] (sha1 (str "double " this)))

  ;; I guess we don't need these two? Try to design better...
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

(defprotocol IDecomposeable
  (decompose [this]))

(extend-protocol IDecomposeable
  clojure.lang.APersistentVector
  (decompose [this]
    (cons :vector this))
  clojure.lang.APersistentMap
  (decompose [this]
    (let [sorted-keys (-> this keys sort vec)]
      (list :map sorted-keys (mapv this sorted-keys)))))

(defmulti compose first)
(defmethod compose :vector
  [_ & els]
  (vec els))
(defmethod compose :map
  [_ ks vs]
  (zipmap ks vs))

(defmethod print-method Reference
  [ref pw]
  (.append pw "#dedup.core/Reference ")
  (binding [*out* pw] (pr (.sha ref))))

(defn store
  [ob]
  (if (satisfies? IDecomposeable ob)
    (let [[type & contents] (decompose ob)
          k (sha1 (s/join " " (cons (name type)
                                    (map #(.sha (store %)) contents))))]
      (kv/put k (cons type contents))
      (->Reference k))
    (let [k (hash ob)]
      (kv/put k ob)
      (->Reference k))))

(defn materialize
  [ref]
  (let [ob (kv/get (.sha ref))]
    (if (sequential? ob)
      (let [[type & contents] ob]
        (compose (cons type (map materialize contents))))
      ob)))

(defn standard-types
  [ob]
  (wk/postwalk (fn [x] (if (instance? Integer x) (long x) x)) ob))

(defn -main
  "Reads a bunch of JSON objects from STDIN and prints the dedup'd data store."
  []
  (doseq [line (line-seq (java.io.BufferedReader. *in*))]
    (store (standard-types (json/parse-string line))))
  (prn @kv/store))