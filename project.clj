(defproject duct/queue.kafka "0.0.1"
   :description "Integrant methods for connecting to Kafka"
   :url "https://github.com/teaforthecat/duct-kafka"
   :license {:name "Eclipse Public License"
             :url "http://www.eclipse.org/legal/epl-v10.html"}
   :dependencies [[org.clojure/clojure "1.8.0"]
                  [integrant "0.3.3"]
                  [ymilky/franzy "0.0.1"]
                  [ymilky/franzy-nippy "0.0.1"]
                  [com.taoensso/encore "2.91.0"]
                  [org.clojure/core.async "0.4.490"]])
