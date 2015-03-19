(ns clj-btable.core
  "A binary serialization format for sparse, labeled two-dimensional
   numeric datasets ('binary tables'). btables are significantly
   more space-efficient than CSVs for sparse datasets, as well as
   faster to read and write.

   btables store their labels internally as a delimited string, and encode
   the row index/value of each nonzero cell.

   btables are *not* a drop-in replacement for all datasets stored as CSV:
   the increases in efficiency is proportional to the sparsity of the
   dataset. For a pathological fully-nonzero dataset, the space occupied
   can be much larger than a CSV!

   Disk file format:
     - Integer format version (4 bytes)
     - Integer character count of labels string, including separating commas (4 bytes)
     - Label string as sequence of 2-byte UTF-16 characters, comma separated
     Then, for each row:
       - Integer number of nonzero values in row (4 bytes)
       - Sequential 'pairs' of index/value for each nonzero cell
         (4 bytes integer index, 8 byte double value)

   All numeric values are written big-endian.

   An example row sequence of:

     [[38 0 0 47 0]
      [0 63 79 0 0]]

   would be written to disk as:

     2 0 38 3 47 2 1 63 2 79"
  (:require [clojure.java.io :as io]
            [clojure.string :as string])
  (import (java.io DataInputStream EOFException)
          (io.framed BTableWriter)))

(def version 0)

(def sep BTableWriter/SEP)
(def sep-re (re-pattern (str sep)))

(defn write [x labels rows]
  (let [f (io/file x)
        labels' (string/join sep labels)]
    (BTableWriter/write f labels' rows)))

;;

(defn- data-input-stream [x]
  (DataInputStream. (io/input-stream x)))

(defn- read-int
  "Read an integer from an input stream, or return
   nil on EOF"
  [is]
  (try (.readInt is)
    (catch EOFException e nil)))

(defn- read-str
  "Read <len> characters from a DataInputStream and concatenate them
   into a string"
  [is len]
  (loop [cs (transient [])
         remaining len]
    (if (> remaining 0)
      (recur (conj! cs (.readChar is)) (dec remaining))
      (apply str (persistent! cs)))))

(defn- read-labels
  "Read a sequence of labels from the current position of a DataInputStream,
   statefully advancing its counter"
  [is]
  (let [labels-len (.readInt is)]
    (-> (read-str is labels-len)
        (string/split sep-re))))

(defn labels
  "Read a sequence of labels from a table on disk
   x - coercible to clojure.java.io/input-stream"
  [x]
  (with-open [is (data-input-stream x)]
    (.readInt is) ; Version
    (read-labels is)))

(defn materialize
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
  [is nvals]
  (map (fn [_]
         (let [idx (.readInt is)
               v (.readDouble is)]
            [idx v]))
       (range nvals)))

(defn- read-rows [is feature-count]
  (lazy-seq
    (if-let [nvals (read-int is)]
      (cons (materialize feature-count (read-indices is nvals))
            (read-rows is feature-count))
      (.close is))))

(defn rows
  "Return a lazy sequence of rows from a table on disk
   x - coercible to clojure.java.io/input-stream"
  [x]
  (let [is (data-input-stream x)
        version (.readInt is)
        labels (read-labels is)
        feature-count (count labels)]
    (read-rows is feature-count)))
