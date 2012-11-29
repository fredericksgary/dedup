(ns dedup.sha1)

;; pasted these three from a gist I googled

(defn- get-hash [type data]
  (.digest (java.security.MessageDigest/getInstance type) (.getBytes data)))

(defn- sha1-hash [data]
  (get-hash "sha1" data))

(defn- get-hash-str [data-bytes]
  (apply str
         (map
          #(.substring
            (Integer/toString
             (+ (bit-and % 0xff) 0x100) 16) 1)
          data-bytes)))

(def sha1 (comp get-hash-str sha1-hash))