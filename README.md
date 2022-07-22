# Lightweight data provider (1.1.1)

# Overview
This component serves as a data provider for [th2-data-services](https://github.com/th2-net/th2-data-services). It will connect to the cassandra database via [cradle api](https://github.com/th2-net/cradleapi) and expose the data stored in there as REST resources.
This component is similar to [rpt-data-provider](https://github.com/th2-net/th2-rpt-data-provider) but the last one contains additional GUI-specific logic.

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
