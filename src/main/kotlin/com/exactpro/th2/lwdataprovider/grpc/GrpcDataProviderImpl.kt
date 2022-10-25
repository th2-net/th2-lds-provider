/*******************************************************************************
 * Copyright 2022 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.exactpro.th2.lwdataprovider.grpc

import com.exactpro.cradle.CradleManager
import com.exactpro.cradle.messages.StoredGroupMessageBatch
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.dataprovider.grpc.CradleMessageGroupsRequest
import com.exactpro.th2.dataprovider.grpc.CradleMessageGroupsResponse
import com.exactpro.th2.dataprovider.grpc.DataProviderGrpc
import com.exactpro.th2.dataprovider.grpc.EventResponse
import com.exactpro.th2.dataprovider.grpc.EventSearchRequest
import com.exactpro.th2.dataprovider.grpc.EventSearchResponse
import com.exactpro.th2.dataprovider.grpc.Group
import com.exactpro.th2.dataprovider.grpc.MessageGroupResponse
import com.exactpro.th2.dataprovider.grpc.MessageGroupsSearchRequest
import com.exactpro.th2.dataprovider.grpc.MessageGroupsSearchResponse
import com.exactpro.th2.dataprovider.grpc.MessageSearchRequest
import com.exactpro.th2.dataprovider.grpc.MessageSearchResponse
import com.exactpro.th2.dataprovider.grpc.MessageStream
import com.exactpro.th2.dataprovider.grpc.MessageStreamInfo
import com.exactpro.th2.dataprovider.grpc.MessageStreamsRequest
import com.exactpro.th2.dataprovider.grpc.MessageStreamsResponse
import com.exactpro.th2.lwdataprovider.GrpcEvent
import com.exactpro.th2.lwdataprovider.GrpcResponseHandler
import com.exactpro.th2.lwdataprovider.RequestContext
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import com.exactpro.th2.lwdataprovider.entities.requests.GetEventRequest
import com.exactpro.th2.lwdataprovider.entities.requests.GetMessageRequest
import com.exactpro.th2.lwdataprovider.entities.requests.MessagesGroupRequest
import com.exactpro.th2.lwdataprovider.entities.requests.SseEventSearchRequest
import com.exactpro.th2.lwdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.lwdataprovider.handlers.SearchEventsHandler
import com.exactpro.th2.lwdataprovider.handlers.SearchMessagesHandler
import com.exactpro.th2.lwdataprovider.metrics.CRADLE_BATCHES_COUNTER
import com.exactpro.th2.lwdataprovider.metrics.CRADLE_DATA_SIZE_BYTE_COUNTER
import com.exactpro.th2.lwdataprovider.metrics.CradleSearchMessageMethod.MESSAGES
import com.exactpro.th2.lwdataprovider.metrics.CradleSearchMessageMethod.MESSAGES_FROM_GROUP
import com.exactpro.th2.lwdataprovider.metrics.CradleSearchMessageMethod.SINGLE_MESSAGE
import com.exactpro.th2.lwdataprovider.metrics.LOAD_MESSAGES_FROM_CRADLE_COUNTER
import com.exactpro.th2.lwdataprovider.metrics.RequestIdPool
import com.google.protobuf.TextFormat.shortDebugString
import com.google.protobuf.Timestamp
import com.google.protobuf.util.Timestamps
import io.grpc.stub.ServerCallStreamObserver
import io.grpc.stub.StreamObserver
import io.prometheus.client.Counter
import mu.KotlinLogging
import org.apache.commons.lang3.StringUtils
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantLock
import kotlin.math.max
import kotlin.math.min

open class GrpcDataProviderImpl(
    private val configuration: Configuration,
    private val searchMessagesHandler: SearchMessagesHandler,
    private val messageRouter: MessageRouter<MessageGroupBatch>, //FIXME: added for test
    private val cradleManager: CradleManager, //FIXME: added for test
    private val searchEventsHandler: SearchEventsHandler
): DataProviderGrpc.DataProviderImplBase() {

    companion object {
        private val LOGGER = KotlinLogging.logger { }

        private fun MessageGroupResponse.toStreamId() =
            StreamId(messageId.connectionId.sessionAlias, messageId.direction)

        private fun RawMessage.toStreamId() = with(metadata.id) { StreamId(connectionId.sessionAlias, direction) }

        private fun Timestamp.compare(timestamp: Timestamp) = when (val result = seconds.compareTo(timestamp.seconds)) {
            0 -> nanos.compareTo(timestamp.nanos)
            else -> result
        }

        private fun max(t1: Timestamp, t2: Timestamp) = if (t1.compare(t2) > 0) t1 else t2
        private fun min(t1: Timestamp, t2: Timestamp) = if (t1.compare(t2) < 0) t1 else t2
    }

    override fun getEvent(request: EventID?, responseObserver: StreamObserver<EventResponse>?) {
        checkNotNull(request)
        checkNotNull(responseObserver)

        LOGGER.info { "Getting event with ID $request" }

        val queue = ArrayBlockingQueue<GrpcEvent>(5)
        val requestParams = GetEventRequest.fromEventID(request)
        val grpcResponseHandler = GrpcResponseHandler(queue)
        val context = GrpcEventRequestContext(grpcResponseHandler)
        searchEventsHandler.loadOneEvent(requestParams, context)
        processSingle(responseObserver, context) {
            it.event?.let { event -> responseObserver.onNext(event) }
        }
    }

    override fun getMessage(request: MessageID?, responseObserver: StreamObserver<MessageGroupResponse>?) {
        checkNotNull(request)
        checkNotNull(responseObserver)

        LOGGER.info { "Getting message with ID $request" }

        val queue = ArrayBlockingQueue<GrpcEvent>(5)

        val requestParams = GetMessageRequest(request)
        val grpcResponseHandler = GrpcResponseHandler(queue)
        val context = GrpcMessageRequestContext(grpcResponseHandler, cradleSearchMessageMethod = SINGLE_MESSAGE)
        searchMessagesHandler.loadOneMessage(requestParams, context)
        processSingle(responseObserver, context) {
            if (it.message != null && it.message.hasMessage()) {
                responseObserver.onNext(it.message.message)
            }
        }
    }

    private fun <T> processSingle(responseObserver: StreamObserver<T>,
                                context: RequestContext<GrpcEvent>, sender: (GrpcEvent) -> Unit) {
        val value = context.channelMessages.take()
        if (value.error != null) {
            responseObserver.onError(value.error)
        } else {
            sender.invoke(value)
            responseObserver.onCompleted()
        }
        context.contextAlive = false
        context.channelMessages.closeStream()
    }

    override fun searchEvents(request: EventSearchRequest, responseObserver: StreamObserver<EventSearchResponse>) {
        val queue = ArrayBlockingQueue<GrpcEvent>(configuration.responseQueueSize)
        val requestParams = SseEventSearchRequest(request)
        LOGGER.info { "Loading events $requestParams" }

        val grpcResponseHandler = GrpcResponseHandler(queue)
        val context = GrpcEventRequestContext(grpcResponseHandler)
        val serverCallStreamObserver = MetricServerCallStreamObserver(responseObserver, context.sendResponseCounter) { 1 }
        searchEventsHandler.loadEvents(requestParams, context)
        processResponse(serverCallStreamObserver, context, accumulator = StatelessAccumulator {
            if (it.event != null) {
                EventSearchResponse.newBuilder().setEvent(it.event).build()
            } else {
                null
            }
        })
    }

    override fun getMessageStreams(request: MessageStreamsRequest?, responseObserver: StreamObserver<MessageStreamsResponse>?) {
        LOGGER.info { "Extracting message streams" }
        val streamsRsp = MessageStreamsResponse.newBuilder()
        for (name in searchMessagesHandler.extractStreamNames()) {
            val currentBuilder = MessageStream.newBuilder().setName(name)
            streamsRsp.addMessageStream(currentBuilder.setDirection(Direction.SECOND))
            streamsRsp.addMessageStream(currentBuilder.setDirection(Direction.FIRST))
        }
        responseObserver?.apply {
            onNext(streamsRsp.build())
            onCompleted()
        }
    }

    override fun searchMessages(request: MessageSearchRequest, responseObserver: StreamObserver<MessageSearchResponse>) {
        val queue = ArrayBlockingQueue<GrpcEvent>(configuration.responseQueueSize)
        val requestParams = SseMessageSearchRequest(request)
        LOGGER.info { "Loading messages $requestParams" }
        val grpcResponseHandler = GrpcResponseHandler(queue)
        val context = GrpcMessageRequestContext(grpcResponseHandler, maxMessagesPerRequest = configuration.bufferPerQuery, cradleSearchMessageMethod = MESSAGES)
        val loadingStep = context.startStep("messages_loading")
        val serverCallStreamObserver = MetricServerCallStreamObserver(responseObserver, context.sendResponseCounter) { message.messageItemCount }
        searchMessagesHandler.loadMessages(requestParams, context, configuration)
        try {
            processResponse(serverCallStreamObserver, context, loadingStep::finish, StatelessAccumulator { it.message } )
        } catch (ex: Exception) {
            loadingStep.finish()
            throw ex
        }
    }

    override fun loadCradleMessageGroups(
        request: CradleMessageGroupsRequest,
        responseObserver: StreamObserver<CradleMessageGroupsResponse>
    ) {
        val requestId = RequestIdPool.getId()
        try {
            val name = MESSAGES_FROM_GROUP.name
            val messageCounter = LOAD_MESSAGES_FROM_CRADLE_COUNTER.labels(requestId, name)
            val batchSizeCounter = CRADLE_DATA_SIZE_BYTE_COUNTER.labels(requestId)
            val batchCounter = CRADLE_BATCHES_COUNTER.labels(requestId)
            val accumulator = hashMapOf<StreamId, MessageStreamInfo.Builder>()

            val from =
                if (request.hasStartTimestamp()) request.startTimestamp.toInstant() else error("missing start timestamp")
            val to = if (request.hasEndTimestamp()) request.endTimestamp.toInstant() else error("missing end timestamp")
            val groups = request.messageGroupList.asSequence()
                .map(Group::getName)
                .filterNot(StringUtils::isBlank)
                .toSet()
                .also {
                    check(it.isNotEmpty()) {
                        "group name cannot be empty"
                    }
                }

            var batch = MessageGroupBatch.newBuilder()
            var size = 0L
            groups.asSequence()
                .map { group -> cradleManager.storage.getGroupedMessageBatches(group, from, to).iterator() }
                .flatMap(Iterator<StoredGroupMessageBatch>::asSequence)
                .onEach { storedBatch ->
                    batchCounter.inc()
                    messageCounter.inc(storedBatch.messageCount.toDouble())
                    batchSizeCounter.inc(storedBatch.batchSize.toDouble())
                }.filter { storedBatch ->
                    storedBatch.lastTimestamp > to
                } // TODO: optional sort here
                .map(StoredGroupMessageBatch::getMessages)
                .flatMap(Collection<StoredMessage>::asSequence)
                .forEach { storedMessage ->
                    if (storedMessage.timestamp < from || storedMessage.timestamp >= to) {
                        return@forEach
                    }

                    storedMessage.toRawMessage().also { rawMessage ->
                        accumulator.compute(rawMessage.toStreamId()) { streamId, streamInfo ->
                            streamInfo?.apply {
                                numberOfMessages += 1
                                maxTimestamp = max(maxTimestamp, rawMessage.metadata.timestamp)
                                minTimestamp = min(minTimestamp, rawMessage.metadata.timestamp)
                                maxSequence = max(maxSequence, rawMessage.metadata.id.sequence)
                                minSequence = min(minSequence, rawMessage.metadata.id.sequence)
                            } ?: MessageStreamInfo.newBuilder().apply {
                                sessionAlias = streamId.sessionAlias
                                direction = streamId.direction

                                numberOfMessages = 1
                                maxTimestamp = rawMessage.metadata.timestamp
                                minTimestamp = rawMessage.metadata.timestamp
                                maxSequence = rawMessage.metadata.id.sequence
                                minSequence = rawMessage.metadata.id.sequence
                            }
                        }

                        batch.addGroups(rawMessage.toMessageGroup())
                    }

                    size += storedMessage.content.size

                    if (size >= configuration.batchSize) {
                        messageRouter.send(batch.build(), "to_codec")

                        batch = MessageGroupBatch.newBuilder()
                        size = 0L
                    }
                }

            if (size != 0L) {
                messageRouter.send(batch.build(), "to_codec")
            }

            responseObserver.onNext(CradleMessageGroupsResponse.newBuilder().apply {
                messageIntervalInfoBuilder.apply {
                    this.startTimestamp = request.startTimestamp
                    this.endTimestamp = request.endTimestamp
                    accumulator.asSequence()
                        .map { (_, value) -> value.build() }
                        .forEach(this::addMessagesInfo)
                }
            }.build())
            responseObserver.onCompleted()

        } catch (e: Exception) {
            LOGGER.error(e) { "Load group request failure, request ${shortDebugString(request)}" }
        } finally {
            RequestIdPool.releaseId(requestId)
        }


//        val queue = ArrayBlockingQueue<GrpcEvent>(configuration.responseQueueSize)
//        val requestParams = MessagesGroupRequest.fromGrpcRequest(request)
//        LOGGER.info { "Loading messages groups $requestParams" }
//        val grpcResponseHandler = GrpcResponseHandler(queue)
//        val context = GrpcMessageRequestContext(grpcResponseHandler, maxMessagesPerRequest = configuration.bufferPerQuery, cradleSearchMessageMethod = MESSAGES_FROM_GROUP)
//        val loadingStep = context.startStep("messages_group_loading")
//        val serverCallStreamObserver = MetricServerCallStreamObserver(responseObserver, context.sendResponseCounter) {
//            messageIntervalInfo.messagesInfoList.asSequence()
//                .map(MessageStreamInfo::getNumberOfMessages)
//                .sum()
//        }
//        try {
//            searchMessagesHandler.loadMessageGroups(requestParams, context)
//            processResponse(serverCallStreamObserver, context, loadingStep::finish, MessageIntervalInfoAccumulator {
//                CradleMessageGroupsResponse.newBuilder().apply {
//                    messageIntervalInfoBuilder.apply {
//                        this.startTimestamp = request.startTimestamp
//                        this.endTimestamp = request.endTimestamp
//                        forEach(this::addMessagesInfo)
//                    }
//                }.build()
//            })
//        } catch (ex: Exception) {
//            loadingStep.finish()
//            throw ex
//        }
    }

    override fun searchMessageGroups(
        request: MessageGroupsSearchRequest,
        responseObserver: StreamObserver<MessageGroupsSearchResponse>
    ) {
        val queue = ArrayBlockingQueue<GrpcEvent>(configuration.responseQueueSize)
        val requestParams = MessagesGroupRequest.fromGrpcRequest(request)
        LOGGER.info { "Loading messages groups $requestParams" }
        val grpcResponseHandler = GrpcResponseHandler(queue)
        val context = GrpcMessageRequestContext(grpcResponseHandler, maxMessagesPerRequest = configuration.bufferPerQuery, cradleSearchMessageMethod = MESSAGES_FROM_GROUP)
        val loadingStep = context.startStep("messages_group_loading")
        val serverCallStreamObserver = MetricServerCallStreamObserver(responseObserver, context.sendResponseCounter) { collection.messagesList.asSequence()
            .flatMap(MessageGroupResponse::getMessageItemList)
            .count() }
        try {
            searchMessagesHandler.loadMessageGroups(requestParams, context)
            processResponse(serverCallStreamObserver, context, loadingStep::finish, MessageGroupsAccumulator(configuration.responseBatchSize))
        } catch (ex: Exception) {
            loadingStep.finish()
            throw ex
        }
    }

    internal class MessageIntervalInfoAccumulator<T>(
        private val transform: Collection<MessageStreamInfo.Builder>.() -> T
    ) : Accumulator<T> {
        private val accumulator = ConcurrentHashMap<StreamId, MessageStreamInfo.Builder>()

        override fun accumulateAndGet(event: GrpcEvent): T? {
            event.message?.message?.let { groupResponse ->
                if (!groupResponse.hasMessageId()) {
                    LOGGER.warn { "Message response without id ${shortDebugString(event.message)}" }
                    return null
                }

                accumulator.compute(groupResponse.toStreamId()) { streamId, streamInfo ->
                    streamInfo?.apply {
                        numberOfMessages += 1
                        maxTimestamp = if (Timestamps.compare(maxTimestamp, groupResponse.timestamp) < 0) groupResponse.timestamp else maxTimestamp
                        minTimestamp = if (Timestamps.compare(minTimestamp, groupResponse.timestamp) < 0) minTimestamp else groupResponse.timestamp
                        maxSequence = max(maxSequence, groupResponse.messageId.sequence)
                        minSequence = min(minSequence, groupResponse.messageId.sequence)
                    } ?: MessageStreamInfo.newBuilder().apply {
                        sessionAlias = streamId.sessionAlias
                        direction = streamId.direction
                        numberOfMessages = 1
                        maxTimestamp = groupResponse.timestamp
                        minTimestamp = groupResponse.timestamp
                        maxSequence = groupResponse.messageId.sequence
                        minSequence = groupResponse.messageId.sequence
                    }
                }
            }
            return null
        }

        override fun get(): T = accumulator.values.transform()

    }

    internal data class StreamId(val sessionAlias: String, val direction: Direction)

    private class MessageGroupsAccumulator(private val batchSize: Int) : Accumulator<MessageGroupsSearchResponse> {
        private val lock = ReentrantLock()
        private val list = mutableListOf<MessageGroupResponse>()

        override fun accumulateAndGet(event: GrpcEvent): MessageGroupsSearchResponse? {
            event.message?.let {
                try {
                    lock.lock()
                    list.add(event.message.message)
                    if (list.size >= batchSize) {
                        return list.createResponse().also {
                            list.clear()
                        }
                    }
                } finally {
                    lock.unlock()
                }

                LOGGER.trace { "Sending message ${it.message.messageId.toStoredMessageId()}" }
            }
            return null
        }

        override fun get(): MessageGroupsSearchResponse? {
            try {
                lock.lock()
                if (list.isNotEmpty()) {
                    return list.createResponse()
                }
            } finally {
                lock.unlock()
            }
            return null
        }

        private fun List<MessageGroupResponse>.createResponse(): MessageGroupsSearchResponse = MessageGroupsSearchResponse.newBuilder().apply {
                collectionBuilder.apply {
                    addAllMessages(this@createResponse)
                }
            }.build()
    }

    protected open fun onCloseContext(requestContext: RequestContext<GrpcEvent>) {
        requestContext.contextAlive = false
    }

    protected open fun <T> processResponse(
        responseObserver: ServerCallStreamObserver<T>,
        context: RequestContext<GrpcEvent>,
        onFinished: () -> Unit = {},
        accumulator: Accumulator<T>,
    ) {
        val grpcResponseHandler = context.channelMessages
        var inProcess = true
        while (inProcess) {
            val event = grpcResponseHandler.take()
            if (event.close) {
                accumulator.get()?.let { responseObserver.onNext(it) }
                responseObserver.onCompleted()
                onCloseContext(context)
                grpcResponseHandler.closeStream()
                inProcess = false
                onFinished()
                LOGGER.info { "Stream finished" }
            } else if (event.error != null) {
                responseObserver.onError(event.error)
                onCloseContext(context)
                onFinished()
                grpcResponseHandler.closeStream()
                inProcess = false
                LOGGER.warn { "Stream finished with exception" }
            } else {
                accumulator.accumulateAndGet(event)?.let {  responseObserver.onNext(it) }
                context.onMessageSent()
            }
        }
    }

    interface Accumulator<T>{
        fun accumulateAndGet(event: GrpcEvent): T?
        fun get(): T?
    }

    private class StatelessAccumulator<T>(
        private val converter: (GrpcEvent) -> T?
    ) : Accumulator<T> {
        override fun accumulateAndGet(event: GrpcEvent): T? = converter.invoke(event)
        override fun get(): T? = null
    }

    private class MetricServerCallStreamObserver<T> (
        streamObserver: StreamObserver<T>,
        private val metric: Counter.Child,
        private val count: T.() -> Number,
    ): ServerCallStreamObserver<T>() {
        private val origin = streamObserver as ServerCallStreamObserver<T>

        override fun onNext(value: T?) {
            origin.onNext(value)
            value?.let {
                metric.inc(value.count().toDouble())
            }
        }

        override fun onError(t: Throwable?) { origin.onError(t) }

        override fun onCompleted() { origin.onCompleted() }

        override fun isReady(): Boolean = origin.isReady

        override fun setOnReadyHandler(onReadyHandler: Runnable?) { origin.setOnReadyHandler(onReadyHandler) }

        override fun disableAutoInboundFlowControl() { origin.disableAutoInboundFlowControl() }

        override fun request(count: Int) { origin.request(count) }

        override fun setMessageCompression(enable: Boolean) { origin.setMessageCompression(enable) }

        override fun isCancelled(): Boolean = origin.isCancelled

        override fun setOnCancelHandler(onCancelHandler: Runnable?) { origin.setOnCancelHandler(onCancelHandler) }

        override fun setCompression(compression: String?) { origin.setCompression(compression) }
    }

}