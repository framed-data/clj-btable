(defproject io.framed/clj-btable "0.1.2"
  :description
  "A binary serialization format for sparse, labeled 2D numeric datasets"
  :url "https://github.com/framed-data/clj-btable"
  :license {:name "MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [io.framed/std "0.1.2"]]
  :source-paths ["src/clj"]
  :java-source-paths ["src/java"]
  :profiles {:uberjar {:aot :all}}
  :plugins [[codox "0.8.13"]])
