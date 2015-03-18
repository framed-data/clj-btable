(ns clj-btable.core-test
  (:require [clojure.test :refer :all]
            [clj-btable.core :as btable]))

(defn ->tempfile []
  (let [file (java.io.File/createTempFile "clj-btable" ".tmp")]
    (.deleteOnExit file)
    file))

(deftest test-materialize
  (let [n 10
        indices [[2 47.0] [5 62.3] [8 94.56]]]
    (is (= [0.0 0.0 47.0 0.0 0.0 62.3 0.0 0.0 94.56 0.0]
           (btable/materialize n indices)))))

(deftest test-read-write
  (let [labels ["foo" "bar" "baz" "quux"]
        rows  [[22.2 0.0 0.0 47.0]
               [0.0 38.0 57.3 0.0]
               [84.99 92.0 0.0 0.0]]
        table (btable/write (->tempfile) labels rows)]
    (is (= labels (btable/labels table)))
    (is (= rows (btable/rows table)))))
