# Lightweight data provider (2.13.0)

# Overview
This component serves as a data provider for [th2-data-services](https://github.com/th2-net/th2-data-services). It will connect to the cassandra database via [cradle api](https://github.com/th2-net/cradleapi) and expose the data stored in there as REST resources.
This component is similar to [rpt-data-provider](https://github.com/th2-net/th2-rpt-data-provider) but the last one contains additional GUI-specific logic.

# API

You can download OpenAPI schema from here: `http://<provider_address>:<port>/openapi`

You can view the endpoints documentation from here: `http://<provider_address>:<port>/redoc`

You can see the Swagger UI from here: `http://<provider_address>:<port>/swagger`

### REST

`http://localhost:8080/event/{id}` - returns a single event with the specified id.

Example: `http://localhost:8080/event/book:scope:20221031130000000000000:eventId`

Example with batch: `http://localhost:8080/event/book:scope:20221031130000000000000:batchId>book:scope:20221031130000000000000:eventId`

`http://localhost:8080/message/{id}` - returns a single message with the specified id

Example: `http://localhost:8080/message/book:session_alias:<direction>:20221031130000000000000:1`

**direction** - `1` - first, `2` - second

##### Timestamp

Timestamp in HTTP request might be specified either in milliseconds or in nanoseconds.
The value will be interpreted as milliseconds if it is less than `1_000_000_000 ^ 2`.
Otherwise, it will be interpreted as nanoseconds.

##### SSE requests API

`http://localhost:8080/search/sse/events` - create a sse channel of event metadata that matches the filter. Accepts following query parameters:
- `startTimestamp` - number, unix timestamp in milliseconds or nanos - Sets the search starting point. **Required**
- `endTimestamp` - number, unix timestamp in milliseconds or nanos - Sets the timestamp to which the search will be performed, starting with `startTimestamp`. **Required**.
- `parentEvent` - text - parent event id of expected child-events.
- `searchDirection` - the direction for search (_next_,_previous_). Default, **next**
- `resultCountLimit` - limit the result responses
- `bookId` - book ID for requested events (*required)
- `scope` - scope for requested events (*required)
- `filters` - set of filters to apply. Example, `filters=name,type`


Supported filters:
- `type` - filter by events type
- `name` - filter by events name

Filter parameters:
- `<filter_name>-value`|`<filter_name>-values` filter values to apply. Repeatable
- `<filter_name>-negative` - inverts the filter. _<filter_name>-negative=true_
- `<filter_name>-conjunct` - if `true` the actual value should match all expected values. _<filter_name>-conjunct=true_



`http://localhost:8080/search/sse/messages` - create a sse channel of messages that matches the filter. Accepts following query parameters:
- `startTimestamp` - number, unix timestamp in milliseconds or nanos - Sets the search starting point. **One of the 'startTimestamp' or 'messageId' must not be null**
- `messageId` - text, accepts multiple values. List of message IDs to restore search. Defaults to `null`. **One of the 'startTimestamp' or 'messageId' must not be null**

- `stream` - text, accepts multiple values - Sets the stream ids to search in. Case-sensitive. **Required**.
  Example: `alias` - requests all direction for alias; `alias:<direction>` - requests specified direction for alias.
- `searchDirection` - `next`/`previous` - Sets the lookup direction. Can be used for pagination. Defaults to `next`.
- `resultCountLimit` - number - Sets the maximum amount of messages to return. Defaults to `null (unlimited)`.
- `endTimestamp` - number, unix timestamp in milliseconds or nanos - Sets the timestamp to which the search will be performed, starting with `startTimestamp`. When `searchDirection` is `previous`, `endTimestamp` must be less then `startTimestamp`. Defaults to `null` (search can be stopped after reaching `resultCountLimit`). 
- `responseFormat` - text, accepts multiple values - sets response formats. Possible values: BASE_64, PROTO_PARSED, JSON_PARSED. default value - BASE_64 & PROTO_PARSED.
- `keepOpen` - keeps pulling for updates until have not found any message outside the requested interval. Disabled by default
- `bookId` - book ID for requested messages (*required)

`http://localhost:8080/search/sse/messages/group` - creates an SSE channel of messages that matches the requested group for the requested time period
- `startTimestamp` - number, unix timestamp in milliseconds or nanos - Sets the search starting point. **Must not be null**
- `endTimestamp` - number, unix timestamp in milliseconds or nanos - Sets the search ending point. **Must not be null**
- `group` - the repeatable parameter java.time.Instantwith group names to request. **At least one must be specified**
- `keepOpen` - keeps pulling for updates until have not found any message outside the requested interval. Disabled by default
- `bookId` - book ID for requested messages (*required)
- `responseFormat` - text, accepts multiple values - sets response formats. Possible values: BASE_64, PROTO_PARSED, JSON_PARSED. default value - BASE_64 & PROTO_PARSED.
Example: `http://localhost:8080/search/sse/messages/group?group=A&group=B&startTimestamp=15600000&endTimestamp=15700000`

`http://localhost:8080/search/sse/page-infos` - creates an SSE channel of page infos that matches the requested book id for the requested time period
- `startTimestamp` - number, unix timestamp in milliseconds or nanos - Sets the search starting point. **Must not be null**
- `endTimestamp` - number, unix timestamp in milliseconds or nanos - Sets the search ending point. **Must not be null**
- `bookId` - book ID for requested messages (*required)
- `resultCountLimit` - number - Sets the maximum amount of page events to return. Defaults to `null (unlimited)`.
Example: `http://localhost:8080/search/sse/page-infos?startTimestamp=15600000&endTimestamp=15700000&bookId=book1`

Elements in channel match the format sse:
```
event: 'event' / 'message' | 'close' | 'error' | 'keep_alive' | 'page_info'
data: 'Event metadata object' / 'message' | 'Empty data' | 'HTTP Error code' | 'Empty data' | 'page info'
id: event / message id | null | null | null | page id
```


# Configuration
schema component description example (lw-data-provider.yml):

## CR V1

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
#   batchSizeBytes: 256KB # the max size of the batch in bytes. You can use 'MB,KB' suffixes or a plain int value
#   codecUsePinAttributes: true # send raw message to specified codec (true) or send to all codecs (false) 
#   responseFormats: string list # resolve data for selected formats only. (allowed values: BASE_64, PARSED)
#   flushSseAfter: 0 # number of SSE emitted before flushing data to the output stream. 0 means flush after each event
#   gzipCompressionLevel: -1 # integer value of gzip compression level. This option is used when user requests data via HTTP with enabled commpression. 
#      * -1: default compression level
#      * 0: no compression
#      * 1: best speed
#      * 9: best compression

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


## CR V2

```yaml
apiVersion: th2.exactpro.com/v2
kind: Th2CoreBox
metadata:
  name: lw-data-provider
spec:
  imageName: ghcr.io/th2-net/th2-lw-data-provider
  imageVersion: 1.0.3
  type: th2-rpt-data-provider
  customConfig:
    hostname: 0.0.0.0 # IP to listen to requests. 
    port: 8080 
    
#   keepAliveTimeout: 5000 # timeout in milliseconds. keep_alive sending frequency
#   maxBufferDecodeQueue: 10000 # buffer size for messages that sent to decode but answers hasn't been received 
#   decodingTimeout: 60000 # timeout expecting answers from codec. 
#   batchSizeBytes: 256KB # the max size of the batch in bytes. You can use 'MB,KB' suffixes or a plain int value
#   validateCradleData: false # validate data loaded from cradle. NOTE: Enabled validation affect performance 
#   codecUsePinAttributes: true # send raw message to specified codec (true) or send to all codecs (false) 
#   responseFormats: string list # resolve data for selected formats only. (allowed values: BASE_64, PARSED)
    

  # pins are used to communicate with codec components to parse message data
  pins:
    mq:
      publishers:
      - name: to_codec
        attributes:
          - to_codec
          - raw
          - publish
      subscribers:
      - name: from_codec
        attributes:
          - from_codec
          - parsed
          - subscribe
        linkTo:
          - box: codec
            pin: from_codec_decode_general
  extendedSettings:
    service:
      enabled: true
      nodePort:  # Required if you use ingress url path
        - name: 'connect'
          targetPort: 8080
#          exposedPort: 30042 # if you need a constant port to be exposed
      ingress: 
        urlPaths: 
          - '/lw-dataprovider/'
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

# Release notes:

## 2.13.0

### Updates:
+ th2 gradle plugin `0.1.6`
+ kotlin: `1.9.23`
+ kotlin-logging: `6.0.9`
+ javalin: `6.3.0`
+ openapi: `6.3.0`
+ micrometer: `1.14.1`

### Migrated:
+ from: `net.jpountz.lz4:lz4:1.3.0` to: `org.lz4:lz4-java:1.8.0`

## 2.12.0

+ Conversion to JSON in HTTP handlers is executed in the separate executor. 
  The executor has `convThreadPoolSize` number of threads
+ Added `/download/events` endpoint to download events as file in JSONL format
+ Added `EVENTS` resource option for `/download` task endpoint

## 2.11.1

+ Updated:
  + th2 gradle plugin: `0.1.3`
  + cradle api: `5.4.4-dev`

## 2.11.0

+ Updated:
  + th2 gradle plugin `0.1.1`
  + common: `5.14.0-dev`
  + cradle api: `5.4.2-dev`
  + kotlin-logging: `5.1.4`
  + micrometer-bom: `1.13.3`

## 2.10.0

+ Updated:
  + th2 gradle plugin `0.0.8`
  + common: `5.12.0-dev`

## 2.9.0
+ Migrated to th2 gradle plugin `0.0.6`
+ Added swagger library
+ Updated:
  + bom: `4.6.1-dev`
  + cradle api: `5.3.0-dev`
  + common: `5.11.0-dev`
  + micrometer-bom: `1.12.5`
  + jetty-bom: `11.0.20`
  + javalin: `5.6.5`
  + javalin-openapi: `5.6.4`

## 2.7.0

+ Updated cradle api: `5.2.0-dev`
+ Updated common: `5.8.0-dev`

## 2.6.0

+ Add download task endpoints:
  + POST `/download` - register task
  + GET `/download/{taskID}` - execute task
  + GET `/download/{taskID}/status` - get task status
  + DELETE `/download/{taskID}` - remove task
+ Add parameter `downloadTaskTTL` to clean up the completed or not started task after the specified time in milliseconds. 1 hour by default.
+ Add `EXTERNAL_CONTEXT_PATH` env variable to inform provider about external context that is used in requests

## 2.5.2

+ Fix possible deadlock in case the response queue is filled up with keep-alive events 
  and there is no enough space to accumulate the batch for codec

## 2.5.1
+ Enabled [Cassandra driver metrics](https://docs.datastax.com/en/developer/java-driver/4.10/manual/core/metrics/)

## 2.5.0

### Updated:
+ common: `5.7.1-dev`
+ grpc-lw-data-provider: `2.3.0`

## 2.4.1

### Fixed:

+ custom JSON serialization did not do proper escaping for strings

## 2.4.0

+ Added `batchSizeBytes` parameter to limit batch size by size in bytes rather than count of messages.
+ Added `validateCradleData` parameter to enable/disable validation logic for data loaded from cradle.
  Currently, managed validation logic includes check for message sequence and timestamp inside a group batch.  
+ Parameters `batchSize` and `groupRequestBuffer` removed.
  The maximum batch size in messages is computed based on `bufferPerQuery` or `maxBufferDecodeQueue` if previous parameter is not set.

## 2.3.1

### Fixed:

+ check order of messages in the whole group batch instead of according to streams (session alias + direction).

## 2.3.0

### Feature

+ Add `gzipCompressionLevel` parameter into configuration

## 2.2.1

+ Migrated to the cradle version with fixed load pages where `removed` field is null problem.
+ Updated cradle: `5.1.4-dev`

## 2.2.0

+ Add support for requesting message groups in reversed order
+ Add filter by stream to gRPC API for group search request

### Fixed:

+ error reporting when executing group search (if error occurs during a call to the Cradle API stream were closed before the error was reported)

## 2.8.0

+ Updated bom: `4.6.0-dev`
+ Updated common: `5.10.0-dev` (recover subscription of channels on failure)

## 2.1.0

+ Updated bom: `4.5.0-dev`
+ Updated common: `5.4.0-dev`
+ Updated kotlin: `1.8.22`

## 2.0.1

+ Removed extra `fields` field from JSON_PARSED message format. 
  + Old: `{"fields":{"a":{"fields":{"b":"1"}}}}`
  + New: `{"fields":{"a":{"b":"1"}}}`