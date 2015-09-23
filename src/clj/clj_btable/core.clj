(ns clj-btable.core
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [framed.std.io :as std.io])
  (import java.io.EOFException
          io.framed.BTableWriter))

(def version
  "The current format version, used as as preamble for each file"
  0)

(def ^:no-doc sep BTableWriter/SEP)
(def ^:no-doc sep-re (re-pattern (str sep)))

(defn write
  "Encode/write a sequence of labels and rows to file-like x

   Ex:
     (def labels [\"login\", \"view_item\", \"purchase\"])
     (def rows [[5.0 3.0 1.0] [2.0 0.0 0.0] [0.0 0.0 0.0]])
     (write \"out.btable\" labels rows)
     ; => #<File out.btable>
  "
  [x labels rows]
  (let [f (io/file x)
        labels' (string/join sep labels)]
    (BTableWriter/write f labels' rows)))

;;

(defn- read-int
  "Read an integer from an input stream, or return
   nil on EOF"
  [istream]
  (try (.readInt istream)
    (catch EOFException e nil)))

(defn- read-str
  "Read <len> characters from a DataInputStream and concatenate them
   into a string"
  [istream len]
  (loop [cs (transient [])
         remaining len]
    (if (> remaining 0)
      (recur (conj! cs (.readChar istream)) (dec remaining))
      (apply str (persistent! cs)))))

(defn- read-labels
  "Read a sequence of labels from the current position of a DataInputStream,
   statefully advancing its counter"
  [istream]
  (let [labels-len (.readInt istream)]
    (-> (read-str istream labels-len)
        (string/split sep-re))))

(defn labels
  "Read a sequence of labels from a table on disk
   x - coercible to clojure.java.io/input-stream"
  [x]
  (with-open [istream (std.io/data-input-stream x)]
    (.readInt istream) ; Version
    (read-labels istream)))

(defn ^:no-doc materialize
  "Fully materialize a row vector given a row length
   and sequence of [index,value] pairs

   Ex:
     (materialize 10 [[2 47] [5 62] [8 94]])
     [0.0 0.0 47 0.0 0.0 62 0.0 0.0 94 0.0]"
  [n indices]
  (let [index-map (into {} indices)]
    (mapv #(get index-map % 0.0) (range n))))

(defn- read-indices
  "Read a specified number of row indices/values out of input
   stream and return them as a list of pairs"
  [istream nvals]
  (map (fn [_]
         (let [idx (.readInt istream)
               v (.readDouble istream)]
            [idx v]))
       (range nvals)))

(defn- read-rows [istream feature-count]
  (lazy-seq
    (if-let [nvals (read-int istream)]
      (cons (materialize feature-count (read-indices istream nvals))
            (read-rows istream feature-count))
      (.close istream))))

(defn rows
  "Return a lazy sequence of rows (vectors of doubles)
   from a BTable on disk

   x - coercible to clojure.java.io/input-stream

   Ex:
    (doseq [row (rows \"out.btable\")]
      (println row))"
  [x]
  (let [istream (std.io/data-input-stream x)
        version (.readInt istream)
        labels (read-labels istream)
        feature-count (count labels)]
    (read-rows istream feature-count)))
