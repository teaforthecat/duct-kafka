(ns duct.queue.kafka
  " Manage a Kafka connection for both producing and consuming.

  When consuming, a message handler is given and offsets are committed after
  each `poll!`, which may be one or more messages.

  Provides:
    - integrant keys for fully managing a Kafka connection.
    - a Boundary protocol with `produce` and `consume` methods.
    - default serialization via `nippy/freeze` and `nippy/thaw`.
  "
  (:require [integrant.core :as ig]
            [taoensso.nippy :as nippy]
            [franzy.serialization.nippy.serializers :as serializers]
            [franzy.serialization.nippy.deserializers :as deserializers]
            [franzy.clients.consumer.client :as kc]
            [franzy.clients.consumer.protocols :refer [poll! subscribe-to-partitions! commit-offsets-async!]]
            [franzy.clients.producer.protocols :refer [send-sync!]]
            [franzy.clients.producer.client :as kp]
            [clojure.core.async :as a]))

(defprotocol Boundary
  (produce
    [conn message]
    [conn message serializer])
  (consume
    [conn topic handler]
    [conn topic handler deserializer]))

(defn ser [msg serializer]
  (-> msg
      (update :key serializer)
      (update :value serializer)))

(defn deser [msg serializer]
  (-> msg
      (update :key serializer)
      (update :value serializer)))

(defn make-producer [opts]
  (kp/make-producer (merge {:value.serializer "org.apache.kafka.common.serialization.ByteArraySerializer"
                            :key.serializer "org.apache.kafka.common.serialization.ByteArraySerializer"}
                           opts)))

(defn make-consumer [opts]
  ;; bug in kc/make-consumer should be config-codec/encode instead of config-codec/decode,
  ;; so to avoid that we need to pass 3 or 4 args
  (kc/make-consumer opts
                    (org.apache.kafka.common.serialization.ByteArrayDeserializer.)
                    (org.apache.kafka.common.serialization.ByteArrayDeserializer.)
                    {}))

(defn consume-thread
  "Consume a Kafka topic or topics from a designated thread.
  `cnsmr` is the result of `kc/make-consumer`
  `topic` can be a string for a single topic or a vector of strings for multiple topics
  `f` will be applied to each message (after deserializer)
  `deserializer` allows a default serialization to be used

  Returns a map with control functions as values
  Usage:
  ((:ctrl c) :pause)
  ((:ctrl c) :resume)
  ((:ctrl c) :halt)
  (a/close! (:thread c))

  "
  [cnsmr topic f deserializer]
  (let [c cnsmr
        state (atom :continue)
        ;;              state    event  new state
        state-machine {:continue {:pause :pausing
                                  :halt :halting}
                       :pausing {:paused :paused}
                       :paused {:resume :resuming
                                :halt :halting}
                       :resuming {:continue :continue}
                       :halting {:halt :halted}}
        step-state (fn [transition]
                     (swap! state #(get-in state-machine [% transition])))
        ;; a string means single topic, a vector of strings means multiple topics
        _ (subscribe-to-partitions! c (if (string? topic) [topic] topic))
        thread (a/go
                 (loop []
                   (case @state
                     :continue
                     (do
                       (doseq [m (poll! c)]
                         (f (deser m deserializer)))
                       (commit-offsets-async! c))
                     :paused
                     (Thread/sleep 0.1)
                     :pausing
                     (do
                       (.pause! c
                                (.assigned-partitions c))
                       (step-state :paused))
                     :resuming
                     (do
                       (.resume! c (.assigned-partitions c))
                       (step-state :continue))
                     :halting
                     (do
                       (.close c)
                       (step-state :halt)))
                   (when (not= :halted @state)
                     (recur))))]

    {:state state
     :thread thread
     :ctrl step-state
     :consumer c}))

(defrecord Conn [producer make-consumer-fn]
  Boundary
  (produce [this message]
    (produce this message nippy/freeze))
  (produce [this message serializer]
    (send-sync! (:producer this) (ser message serializer)))
  (consume [this topic handler]
    (consume this topic handler nippy/thaw))
  (consume [this topic handler deserializer]
    (let [c ((:make-consumer-fn this))]
      (consume-thread c topic handler deserializer))))


(defmethod ig/init-key :duct.queue.kafka/conn [_ opts]
  (->Conn (make-producer opts)
          (fn [] (make-consumer opts))))

(defmethod ig/halt-key! :duct.queue.kafka/conn [_ conn]
  (cond-> conn
    (:producer conn)
    (update :producer #(do (.close %) %))
    (:consumer conn)
    (update :consumer #(do ((:ctrl %) :halt)
                           (a/close! (:thread %))
                           %))))

(defmethod ig/suspend-key! :duct.queue.kafka/conn [_ conn]
  (cond-> conn
    (:consumer conn)
    (update :consumer #((:ctrl %) :pause))))

(defmethod ig/resume-key :duct.queue.kafka/conn [_ conn _ _]
  (cond-> conn
    (:consumer conn)
    (update :consumer #((:ctrl %) :resume))))

(defrecord MockConn [chan]
  Boundary
  (produce [this message]
    (produce this message pr-str))
  (produce [this message serializer]
    (a/put! chan (serializer message)))
  (consume [this topic handler]
    (consume this topic handler read-string))
  (consume [this topic handler deserializer]
    (-> (a/alts!! [chan (a/timeout 1000)])
        first
        deserializer
        ;; the real Conn would return a control map
        handler)))

(defn mock-conn []
  ;; this pub sub would be almost like kafka, but kafka keeps more than 1000 messages
  ;; pub (a/pub c :topic (fn [topic] (dropping-buffer 1000)))
  ;; (a/sub pub topic topic-chan)

  ;; pub sub looks like a lot of work though, and I can't quite work it all
  ;; out in my head right now. Having only one topic is fine for testing
  ;; anyway.
  (->MockConn (a/chan)))



(comment


  ;; start services with docker-compose up

  (def conn
    (ig/init-key :duct.queue.kafka/conn {:bootstrap.servers "localhost:9092"
                                         :group.id "abc"}))

  (def c
    (consume conn
             "queue-123"
             println))

  (produce conn {:key :hello :value "world" :topic "queue-123"})
  ;; see consumer record in repl


  ((:ctrl c) :pause)
  ((:ctrl c) :resume)
  ((:ctrl c) :halt)
  (a/close! (:thread c))

  )
