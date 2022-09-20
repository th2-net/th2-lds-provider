# Lightweight data provider (1.1.1)

# Overview
This component serves as a data provider for [th2-data-services](https://github.com/th2-net/th2-data-services). It will connect to the cassandra database via [cradle api](https://github.com/th2-net/cradleapi) and expose the data stored in there as REST resources.
This component is similar to [rpt-data-provider](https://github.com/th2-net/th2-rpt-data-provider) but the last one contains additional GUI-specific logic.

# Metrics

* th2_ldp_max_decode_message_buffer_size : Gauge - Max decode message buffer capacity. It is common buffer for all requests to decode messages
* th2_ldp_decode_message_buffer_size : Gauge - Actual number of raw message in decode buffer.

* th2_ldp_max_response_buffer_size : Gauge - Max message/event response buffer capacity.
* th2_ldp_response_buffer_size(request_id) : Gauge - Actual number of message/event in response buffer.
  * The request_id label is value from pool active requests.

* th2_ldp_load_messages_from_cradle_total(request_id, cradle_search_message_method) : Counter - Number of messages loaded from cradle. 
  * The request_id label is value from pool active requests.
  * The cradle_search_message_method label has SINGLE_MESSAGE, MESSAGES, MESSAGES_FROM_GROUP values
* th2_ldp_load_events_from_cradle_total(request_id) : Counter - Number of events loaded from cradle.
  * The request_id label is value from pool active requests.

* th2_ldp_send_messages_total(request_id, interface, cradle_search_message_method) : Counter - Send messages via gRPC/HTTP interface. 
  * The request_id label is value from pool active requests.
  * The interface label has HTTP, GRPC values.
  * The cradle_search_message_method label has SINGLE_MESSAGE, MESSAGES, MESSAGES_FROM_GROUP values.
* th2_ldp_send_events_total(request_id, interface) : Counter - Send events via gRPC/HTTP interface.
  * The request_id label is value from pool active requests.
  * The interface label has HTTP, GRPC values

* th2_ldp_grpc_back_pressure_time_seconds(request_id, status) : Counter - Time is seconds which LwDP spent in both status of back pressure
  * The request_id label is value from pool active requests.
  * The status label has ON, OFF values.

* th2_ldp_message_pipeline_hist_time(step) : Histogram - Time spent on each step for a message
  * The step label has cradle, messages_group_loading, messages_loading, decoding, raw_message_parsing, await_queue values

# API

### REST

`http://localhost:8080/event/{id}` - returns a single event with the specified id

`http://localhost:8080/message/{id}` - returns a single message with the specified id

##### SSE requests API

`http://localhost:8080/search/sse/events` - create a sse channel of event metadata that matches the filter. Accepts following query parameters:
- `startTimestamp` - number, unix timestamp in milliseconds - Sets the search starting point. **Required**
- `endTimestamp` - number, unix timestamp in milliseconds - Sets the timestamp to which the search will be performed, starting with `startTimestamp`. **Required**.
- `parentEvent` - text - parent event id of expected child-events.


`http://localhost:8080/search/sse/messages` - create a sse channel of messages that matches the filter. Accepts following query parameters:
- `startTimestamp` - number, unix timestamp in milliseconds - Sets the search starting point. **One of the 'startTimestamp' or 'messageId' must not be null**
- `messageId` - text, accepts multiple values. List of message IDs to restore search. Defaults to `null`. **One of the 'startTimestamp' or 'messageId' must not be null**

- `stream` - text, accepts multiple values - Sets the stream ids to search in. Case-sensitive. **Required**.
- `searchDirection` - `next`/`previous` - Sets the lookup direction. Can be used for pagination. Defaults to `next`.
- `resultCountLimit` - number - Sets the maximum amount of messages to return. Defaults to `null (unlimited)`.
- `endTimestamp` - number, unix timestamp in milliseconds - Sets the timestamp to which the search will be performed, starting with `startTimestamp`. When `searchDirection` is `previous`, `endTimestamp` must be less then `startTimestamp`. Defaults to `null` (search can be stopped after reaching `resultCountLimit`).
- `onlyRaw` - boolean - Disabling decoding messages. If it is true, message body will be empty in all messages. Default `false`
- `responseFormats` - text, accepts multiple values - sets response formats. Possible values: BASE_64, PARSED. default value - BASE_64 & PARSED.

`http://localhost:8080/search/sse/messages/group` - creates an SSE channel of messages that matches the requested group for the requested time period
- `startTimestamp` - number, unix timestamp in milliseconds - Sets the search starting point. **Must not be null**
- `endTimestamp` - number, unix timestamp in milliseconds - Sets the search ending point. **Must not be null**
- `group` - the repeatable parameter with group names to request. **At least one must be specified**
Example: `http://localhost:8080/search/sse/messages/group?group=A&group=B&startTimestamp=15600000&endTimestamp=15700000`


Elements in channel match the format sse:
```
event: 'event' / 'message' | 'close' | 'error' | 'keep_alive'
data: 'Event metadata object' / 'message' | 'Empty data' | 'HTTP Error code' | 'Empty data'
id: event / message id | null | null | null
```


# Configuration
schema component description example (lw-data-provider.yml):
```yaml
apiVersion: th2.exactpro.com/v1
kind: Th2CoreBox
metadata:
  name: lw-data-provider
spec:
  image-name: ghcr.io/th2-net/th2-lw-data-provider
  image-version: 1.0.3
  type: th2-rpt-data-provider
  custom-config:
    hostname: 0.0.0.0 # IP to listen to requests. 
    port: 8080 
    
#   keepAliveTimeout: 5000 # timeout in milliseconds. keep_alive sending frequency
#   maxBufferDecodeQueue: 10000 # buffer size for messages that sent to decode but anwers hasn't been received 
#   decodingTimeout: 60000 # timeout expecting answers from codec. 
#   batchSize: 100 # batch size from codecs
#   codecUsePinAttributes: true # send raw message to specified codec (true) or send to all codecs (false) 
#   responseFormats: string list # resolve data for selected formats only. (allowed values: BASE_64, PARSED)
#   grpcBackPressure: false # enable gRPC backpressure (true) or (false) for disable
#   responseBatchSize: 1000 # number of messages packed to the search message group response 
    

  pins: # pins are used to communicate with codec components to parse message data
    - name: to_codec
      connection-type: mq
      attributes:
        - to_codec
        - raw
        - publish
    - name: from_codec
      connection-type: mq
      attributes:
        - from_codec
        - parsed
        - subscribe
  extended-settings:    
    service:
      enabled: true
      type: NodePort
      endpoints:
        - name: 'grpc'
          targetPort: 8080
      ingress: 
        urlPaths: 
          - '/lw-dataprovider/(.*)'
    envVariables:
      JAVA_TOOL_OPTIONS: "-XX:+ExitOnOutOfMemoryError -Ddatastax-java-driver.advanced.connection.init-query-timeout=\"5000 milliseconds\""
    resources:
      limits:
        memory: 2000Mi
        cpu: 600m
      requests:
        memory: 300Mi
        cpu: 50m
```
