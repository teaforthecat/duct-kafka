# duct-kafka

A duct library that provides a Kafka publisher and consumer in one interface. 

## not released yet

## Usage

In the duct config.edn:

```clojure
{:duct.queue.kafka/conn {:bootstrap.servers "localhost:9092"
                         :group.id "example"}}
 ```
 
 
 
### Producing and consuming

Assuming `conn` is managed by duct and setup as a dependency injected into a
function, then the calls to product and consume would look something like this:

```clojure
(require '[duct.queue.kafka :as k])

(k/produce conn {:topic "abc" :key "hello" :value "world"}) 
(k/consume conn "abc" abc/handler)
 ```

### Serialization

Default serialization is provided via nippy. For something else, pass the
serializer and deserializer as an additional argument to `produce` and
`consume`

```clojure
(k/produce conn
           {:topic "abc" :key "hello" :value "world"} 
           (comp byte-streams/to-byte-array cheshire/generate-string)) 
(k/consume conn
           "abc"
           abc/handler
           (comp cheshire/parse-string byte-streams/to-string))
```
