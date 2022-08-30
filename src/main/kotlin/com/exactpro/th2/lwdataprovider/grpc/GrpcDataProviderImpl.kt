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

import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.dataprovider.grpc.DataProviderGrpc
import com.exactpro.th2.dataprovider.grpc.EventResponse
import com.exactpro.th2.dataprovider.grpc.EventSearchRequest
import com.exactpro.th2.dataprovider.grpc.EventSearchResponse
import com.exactpro.th2.dataprovider.grpc.MessageGroupResponse
import com.exactpro.th2.dataprovider.grpc.MessageSearchRequest
import com.exactpro.th2.dataprovider.grpc.MessageSearchResponse
import com.exactpro.th2.dataprovider.grpc.MessageStream
import com.exactpro.th2.dataprovider.grpc.MessageStreamsRequest
import com.exactpro.th2.dataprovider.grpc.MessageStreamsResponse
import com.exactpro.th2.lwdataprovider.GrpcEvent
import com.exactpro.th2.lwdataprovider.GrpcResponseHandler
import com.exactpro.th2.lwdataprovider.RequestContext
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import com.exactpro.th2.lwdataprovider.entities.requests.GetEventRequest
import com.exactpro.th2.lwdataprovider.entities.requests.GetMessageRequest
import com.exactpro.th2.lwdataprovider.entities.requests.SseEventSearchRequest
import com.exactpro.th2.lwdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.lwdataprovider.handlers.SearchEventsHandler
import com.exactpro.th2.lwdataprovider.handlers.SearchMessagesHandler
import io.grpc.stub.StreamObserver
import mu.KotlinLogging
import java.util.concurrent.ArrayBlockingQueue

open class GrpcDataProviderImpl(
    private val configuration: Configuration,
    private val searchMessagesHandler: SearchMessagesHandler,
    private val searchEventsHandler: SearchEventsHandler
): DataProviderGrpc.DataProviderImplBase() {

    companion object {
        private val logger = KotlinLogging.logger { }
    }

    override fun getEvent(request: EventID?, responseObserver: StreamObserver<EventResponse>?) {
        checkNotNull(request)
        checkNotNull(responseObserver)

        logger.info { "Getting event with ID $request" }

        val queue = ArrayBlockingQueue<GrpcEvent>(5)
        val requestParams = GetEventRequest.fromEventID(request)
        val grpcResponseHandler = GrpcResponseHandler(queue)
        val context = GrpcEventRequestContext(grpcResponseHandler)
        searchEventsHandler.loadOneEvent(requestParams, context)
        processSingle(responseObserver, grpcResponseHandler, context) {
            it.event?.let { event -> responseObserver.onNext(event) }
        }
    }

    override fun getMessage(request: MessageID?, responseObserver: StreamObserver<MessageGroupResponse>?) {
        checkNotNull(request)
        checkNotNull(responseObserver)

        logger.info { "Getting message with ID $request" }

        val queue = ArrayBlockingQueue<GrpcEvent>(5)

        val requestParams = GetMessageRequest(request)
        val grpcResponseHandler = GrpcResponseHandler(queue)
        val context = GrpcMessageRequestContext(grpcResponseHandler)
        searchMessagesHandler.loadOneMessage(requestParams, context)
        processSingle(responseObserver, grpcResponseHandler, context) {
            if (it.message != null && it.message.hasMessage()) {
                responseObserver.onNext(it.message.message)
            }
        }
    }

    private fun <T> processSingle(responseObserver: StreamObserver<T>, grpcResponseHandler: GrpcResponseHandler,
                                context: RequestContext, sender: (GrpcEvent) -> Unit) {
        val value = grpcResponseHandler.buffer.take()
        if (value.error != null) {
            responseObserver.onError(value.error)
        } else {
            sender.invoke(value)
            responseObserver.onCompleted()
        }
        context.contextAlive = false;
        grpcResponseHandler.streamClosed = true
    }

    override fun searchEvents(request: EventSearchRequest?, responseObserver: StreamObserver<EventSearchResponse>?) {
        checkNotNull(request)
        checkNotNull(responseObserver)

        val queue = ArrayBlockingQueue<GrpcEvent>(configuration.responseQueueSize)
        val requestParams = SseEventSearchRequest(request)
        logger.info { "Loading events $requestParams" }

        val grpcResponseHandler = GrpcResponseHandler(queue)
        val context = GrpcEventRequestContext(grpcResponseHandler)
        searchEventsHandler.loadEvents(requestParams, context)
        processResponse(responseObserver, grpcResponseHandler, context) {
            if (it.event != null) {
                EventSearchResponse.newBuilder().setEvent(it.event).build()
            } else {
                null
            }
        }
    }

    override fun getMessageStreams(request: MessageStreamsRequest?, responseObserver: StreamObserver<MessageStreamsResponse>?) {
        logger.info { "Extracting message streams" }
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

    override fun searchMessages(request: MessageSearchRequest?, responseObserver: StreamObserver<MessageSearchResponse>?) {

        checkNotNull(request)
        checkNotNull(responseObserver)

        val queue = ArrayBlockingQueue<GrpcEvent>(configuration.responseQueueSize)
        val requestParams = SseMessageSearchRequest(request)
        logger.info { "Loading messages $requestParams" }
        val grpcResponseHandler = GrpcResponseHandler(queue)
        val context = GrpcMessageRequestContext(grpcResponseHandler, maxMessagesPerRequest = configuration.bufferPerQuery)
        val loadingStep = context.startStep("messages_loading")
        searchMessagesHandler.loadMessages(requestParams, context, configuration)
        try {
            processResponse(responseObserver, grpcResponseHandler, context, loadingStep::finish) { it.message }
        } catch (ex: Exception) {
            loadingStep.finish()
            throw ex
        }
    }

    protected open fun onCloseContext(requestContext: RequestContext) {
        requestContext.contextAlive = false;
    }

    protected open fun <T> processResponse(
        responseObserver: StreamObserver<T>,
        grpcResponseHandler: GrpcResponseHandler,
        context: RequestContext,
        onFinished: () -> Unit = {},
        converter: (GrpcEvent) -> T?
    ) {
        val buffer = grpcResponseHandler.buffer
        var inProcess = true
        while (inProcess) {
            val event = buffer.take()
            if (event.close) {
                responseObserver.onCompleted()
                onCloseContext(context)
                grpcResponseHandler.streamClosed = true
                inProcess = false
                onFinished()
                logger.info { "Stream finished" }
            } else if (event.error != null) {
                responseObserver.onError(event.error)
                onCloseContext(context)
                onFinished()
                grpcResponseHandler.streamClosed = true
                inProcess = false
                logger.warn { "Stream finished with exception" }
            } else {
                converter.invoke(event)?.let {  responseObserver.onNext(it) }
                context.onMessageSent()
            }
        }
    }
}