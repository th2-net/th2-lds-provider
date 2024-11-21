/*
 * Copyright 2022-2024 Exactpro (Exactpro Systems Limited)
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
 */

package com.exactpro.th2.lwdataprovider.grpc

import com.exactpro.cradle.BookId
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.dataprovider.lw.grpc.BooksRequest
import com.exactpro.th2.dataprovider.lw.grpc.BooksResponse
import com.exactpro.th2.dataprovider.lw.grpc.DataProviderGrpc
import com.exactpro.th2.dataprovider.lw.grpc.EventResponse
import com.exactpro.th2.dataprovider.lw.grpc.EventSearchRequest
import com.exactpro.th2.dataprovider.lw.grpc.EventSearchResponse
import com.exactpro.th2.dataprovider.lw.grpc.MessageGroupResponse
import com.exactpro.th2.dataprovider.lw.grpc.MessageGroupsSearchRequest
import com.exactpro.th2.dataprovider.lw.grpc.MessageSearchRequest
import com.exactpro.th2.dataprovider.lw.grpc.MessageSearchResponse
import com.exactpro.th2.dataprovider.lw.grpc.MessageStream
import com.exactpro.th2.dataprovider.lw.grpc.MessageStreamsRequest
import com.exactpro.th2.dataprovider.lw.grpc.MessageStreamsResponse
import com.exactpro.th2.dataprovider.lw.grpc.PageInfoRequest
import com.exactpro.th2.dataprovider.lw.grpc.PageInfoResponse
import com.exactpro.th2.lwdataprovider.CancelableResponseHandler
import com.exactpro.th2.lwdataprovider.GrpcEvent
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import com.exactpro.th2.lwdataprovider.db.DataMeasurement
import com.exactpro.th2.lwdataprovider.entities.exceptions.InvalidRequestException
import com.exactpro.th2.lwdataprovider.entities.requests.GetEventRequest
import com.exactpro.th2.lwdataprovider.entities.requests.GetMessageRequest
import com.exactpro.th2.lwdataprovider.entities.requests.MessagesGroupRequest
import com.exactpro.th2.lwdataprovider.entities.requests.SseEventSearchRequest
import com.exactpro.th2.lwdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.lwdataprovider.entities.requests.SsePageInfosSearchRequest
import com.exactpro.th2.lwdataprovider.entities.responses.Event
import com.exactpro.th2.lwdataprovider.entities.responses.PageInfo
import com.exactpro.th2.lwdataprovider.handlers.GeneralCradleHandler
import com.exactpro.th2.lwdataprovider.handlers.SearchEventsHandler
import com.exactpro.th2.lwdataprovider.handlers.SearchMessagesHandler
import com.exactpro.th2.lwdataprovider.toCradle
import io.grpc.Status
import io.grpc.stub.StreamObserver
import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.commons.lang3.exception.ExceptionUtils
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue

open class GrpcDataProviderImpl(
    protected val configuration: Configuration,
    private val searchMessagesHandler: SearchMessagesHandler,
    private val searchEventsHandler: SearchEventsHandler,
    private val generalCradleHandler: GeneralCradleHandler,
    private val dataMeasurement: DataMeasurement,
) : DataProviderGrpc.DataProviderImplBase() {

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }

    override fun getBooks(request: BooksRequest, responseObserver: StreamObserver<BooksResponse>) {
        LOGGER.info { "Extracting list of books" }
        try {
            val books = generalCradleHandler.getBookIDs().map(BookId::toGrpc)
            responseObserver.onNext(BooksResponse.newBuilder().addAllBookIds(books).build())
            responseObserver.onCompleted()
        } catch (ex: Exception) {
            responseObserver.onError(Status.INTERNAL.withDescription(ex.message).asRuntimeException())
        }
    }

    override fun getEvent(request: EventID, responseObserver: StreamObserver<EventResponse>) {
        LOGGER.info { "Getting event with ID $request" }

        val queue = ArrayBlockingQueue<GrpcEvent>(5)
        val requestParams = GetEventRequest.fromEventID(request)
        val handler = GrpcHandler<Event>(queue) { GrpcEvent(event = it.convertToGrpcEventData()) }
        searchEventsHandler.loadOneEvent(requestParams, handler)
        processSingle(responseObserver, handler, queue) {
            it.event?.let { event -> responseObserver.onNext(event) }
        }
    }

    override fun getMessage(request: MessageID?, responseObserver: StreamObserver<MessageGroupResponse>?) {
        checkNotNull(request)
        checkNotNull(responseObserver)

        LOGGER.info { "Getting message with ID $request" }

        val queue = ArrayBlockingQueue<GrpcEvent>(5)

        val requestParams = GetMessageRequest(request)
        val handler = GrpcMessageResponseHandler(queue, dataMeasurement)
        searchMessagesHandler.loadOneMessage(requestParams, handler, dataMeasurement)
        processSingle(responseObserver, handler, queue) {
            val message = it.message?.get()
            if (message != null && message.hasMessage()) {
                responseObserver.onNext(message.message)
            }
        }
    }

    private fun <T> processSingle(
        responseObserver: StreamObserver<T>,
        handler: CancelableResponseHandler,
        buffer: BlockingQueue<GrpcEvent>,
        sender: (GrpcEvent) -> Unit,
    ) {
        val value = buffer.take()
        if (value.error != null) {
            responseObserver.onError(value.error)
        } else {
            sender.invoke(value)
            responseObserver.onCompleted()
        }
        handler.cancel()
    }

    override fun searchEvents(request: EventSearchRequest, responseObserver: StreamObserver<EventSearchResponse>) {

        val queue = ArrayBlockingQueue<GrpcEvent>(configuration.responseQueueSize)
        val requestParams = SseEventSearchRequest(request)
        LOGGER.info { "Loading events $requestParams" }

        val handler = GrpcHandler<Event>(queue) { GrpcEvent(event = it.convertToGrpcEventData()) }
        searchEventsHandler.loadEvents(requestParams, handler)
        processResponse(responseObserver, queue, handler) {
            if (it.event != null) {
                EventSearchResponse.newBuilder().setEvent(it.event).build()
            } else {
                null
            }
        }
    }

    override fun getMessageStreams(
        request: MessageStreamsRequest,
        responseObserver: StreamObserver<MessageStreamsResponse>
    ) {
        LOGGER.info { "Extracting message streams" }
        if (!request.hasBookId()) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("bookId is required").asRuntimeException())
            return
        }
        val streamsRsp = MessageStreamsResponse.newBuilder()
        for (name in searchMessagesHandler.extractAllStreamNames(request.bookId.toCradle())) {
            val currentBuilder = MessageStream.newBuilder().setName(name)
            streamsRsp.addMessageStream(currentBuilder.setDirection(Direction.SECOND))
            streamsRsp.addMessageStream(currentBuilder.setDirection(Direction.FIRST))
        }
        responseObserver.apply {
            onNext(streamsRsp.build())
            onCompleted()
        }
    }

    override fun searchMessages(
        request: MessageSearchRequest,
        responseObserver: StreamObserver<MessageSearchResponse>
    ) {

        val queue = ArrayBlockingQueue<GrpcEvent>(configuration.responseQueueSize)
        val requestParams = SseMessageSearchRequest(request)
        LOGGER.info { "Loading messages $requestParams" }
        val handler = GrpcMessageResponseHandler(
            queue,
            dataMeasurement,
            configuration.bufferPerQuery,
            requestParams.responseFormats ?: configuration.responseFormats
        )
//        val loadingStep = context.startStep("messages_loading")
        searchMessagesHandler.loadMessages(requestParams, handler, dataMeasurement)
        try {
            processResponse(responseObserver, queue, handler, { /*finish step*/ }) { it.message?.get() }
        } catch (ex: Exception) {
//            loadingStep.finish()
            throw ex
        }
    }

    override fun searchMessageGroups(
        request: MessageGroupsSearchRequest,
        responseObserver: StreamObserver<MessageSearchResponse>
    ) {
        val queue = ArrayBlockingQueue<GrpcEvent>(configuration.responseQueueSize)
        val requestParams = MessagesGroupRequest.fromGrpcRequest(request)
        LOGGER.info { "Loading messages groups $requestParams" }
        val handler = GrpcMessageResponseHandler(queue, dataMeasurement, configuration.bufferPerQuery)
//        val loadingStep = context.startStep("messages_group_loading")
        try {
            searchMessagesHandler.loadMessageGroups(requestParams, handler, dataMeasurement)
            processResponse(responseObserver, queue, handler, { /*finish step*/ }) {
                it.message?.get()?.apply {
                    LOGGER.trace { "Sending message ${this.message.messageId.toStoredMessageId()}" }
                }
            }
        } catch (ex: Exception) {
//            loadingStep.finish()
            throw ex
        }
    }

    override fun getPageInfo(request: PageInfoRequest, responseObserver: StreamObserver<PageInfoResponse>) {
        val queue = ArrayBlockingQueue<GrpcEvent>(configuration.responseQueueSize)
        try {
            val internalRequest = request.run {
                SsePageInfosSearchRequest(
                    if (hasBookId()) bookId.toCradle() else null,
                    if (hasStartTimestamp()) startTimestamp.toInstant() else null,
                    if (hasEndTimestamp()) endTimestamp.toInstant() else null,
                    if (hasResultLimit()) resultLimit.value else null,
                )
            }
            val handler = GrpcHandler<PageInfo>(queue) { GrpcEvent(pageInfo = it.toGrpc()) }
            generalCradleHandler.getPageInfos(
                internalRequest,
                handler
            )
            processResponse(responseObserver, queue, handler) {
                it.pageInfo
            }
        } catch (ex: InvalidRequestException) {
            LOGGER.error(ex) { "invalid request ${request.toJson()}" }
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(ex.message).asRuntimeException())
        } catch (ex: Exception) {
            LOGGER.error(ex) { "cannot load pages for request ${request.toJson()}" }
            responseObserver.onError(
                Status.INTERNAL.withDescription(ExceptionUtils.getMessage(ex)).asRuntimeException()
            )
        }
    }

    internal open fun <T> processResponse(
        responseObserver: StreamObserver<T>,
        buffer: BlockingQueue<GrpcEvent>,
        handler: CancelableResponseHandler,
        onFinished: () -> Unit = {},
        converter: (GrpcEvent) -> T?
    ) {
        var inProcess = true
        while (inProcess) {
            val event = buffer.take()
            if (event.close) {
                responseObserver.onCompleted()
                onClose(handler)
                inProcess = false
                onFinished()
                LOGGER.info { "Stream finished" }
            } else if (event.error != null) {
                responseObserver.onError(event.error)
                onClose(handler)
                onFinished()
                inProcess = false
                LOGGER.warn { "Stream finished with exception" }
            } else {
                converter.invoke(event)?.let { responseObserver.onNext(it) }
            }
        }
    }

    protected fun onClose(handler: CancelableResponseHandler) {
        handler.complete()
    }
}