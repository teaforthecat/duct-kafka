(ns duct.queue.kafka-test
  (:require [clojure.test :refer [deftest testing is]]
            [clojure.core.async :as a]
            [duct.queue.kafka :as k]))

(deftest mocking-test
  (testing "that the provided mock using the k/Boundary protocol would work"
    (let [mock (k/mock-conn)
          msg {:topic "greeting", :key "hello", :value "world"}]
      (k/consume mock "anything" #(is (= msg %)))
      (k/produce mock msg))))
