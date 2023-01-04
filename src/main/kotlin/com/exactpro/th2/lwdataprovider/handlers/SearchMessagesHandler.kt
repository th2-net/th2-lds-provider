/*******************************************************************************
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.lwdataprovider.handlers

import com.exactpro.cradle.Direction
import com.exactpro.cradle.Order
import com.exactpro.cradle.TimeRelation.AFTER
import com.exactpro.cradle.TimeRelation.BEFORE
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageFilterBuilder
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.lwdataprovider.Decoder
import com.exactpro.th2.lwdataprovider.RequestedMessageDetails
import com.exactpro.th2.lwdataprovider.ResponseHandler
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import com.exactpro.th2.lwdataprovider.db.CradleMessageExtractor
import com.exactpro.th2.lwdataprovider.db.DataMeasurement
import com.exactpro.th2.lwdataprovider.entities.internal.ResponseFormat
import com.exactpro.th2.lwdataprovider.entities.requests.GetMessageRequest
import com.exactpro.th2.lwdataprovider.entities.requests.SseMessageSearchRequest
import mu.KotlinLogging
import java.util.concurrent.Executor
import kotlin.math.max
import kotlin.system.measureTimeMillis

class SearchMessagesHandler(
    private val cradleMsgExtractor: CradleMessageExtractor,
    private val decoder: Decoder,
    private val threadPool: Executor,
    private val configuration: Configuration,
) {
    companion object {
        private val logger = KotlinLogging.logger { }
    }

    fun extractStreamNames(): Collection<String> {
        logger.info { "Getting stream names" }
        return cradleMsgExtractor.getStreams();
    }

    fun loadMessages(request: SseMessageSearchRequest, requestContext: MessageResponseHandler, dataMeasurement: DataMeasurement) {

        if (request.stream.isNullOrEmpty() && request.resumeFromIdsList.isNullOrEmpty()) {
            return
        }

        threadPool.execute {
            RootMessagesDataSink(
                requestContext,
                if (request.responseFormats.hasRowOnly()) {
                    RawStoredMessageHandler(requestContext)
                } else {
                    ParsedStoredMessageHandler(requestContext, decoder, dataMeasurement, configuration.batchSize)
                },
                request.resultCountLimit
            ).use { sink ->
                try {
                    if (!request.resumeFromIdsList.isNullOrEmpty()) {
                        request.resumeFromIdsList.forEach { resumeFromId ->
                            sink.canceled?.apply {
                                logger.info { "loading canceled: $message" }
                                return@use
                            }
                            sink.subSink(resumeFromId.streamName, resumeFromId.direction).use { subSink ->
                                val filter = StoredMessageFilterBuilder().apply {
                                    streamName().isEqualTo(resumeFromId.streamName)
                                    direction().isEqualTo(resumeFromId.direction)
                                    if (request.searchDirection == AFTER) {
                                        index().isGreaterThanOrEqualTo(resumeFromId.index)
                                    } else {
                                        index().isLessThanOrEqualTo(resumeFromId.index)
                                        order(Order.REVERSE)
                                    }

                                    modifyFilterBuilderTimestamps(request)
                                    subSink.limit?.let { limit(max(it, 0)) }

                                }.build()


                                val time = measureTimeMillis {
                                    cradleMsgExtractor.getMessages(filter, subSink, dataMeasurement)
                                }
                                logger.info { "Loaded ${subSink.loadedData} messages from DB $time ms" }
                            }
                        }
                    } else {
                        request.stream?.forEach { (stream, direction) ->
                            sink.canceled?.apply {
                                logger.info { "loading canceled: $message" }
                                return@use
                            }

                            sink.subSink(stream, direction).use { subSink ->
                                val filter = StoredMessageFilterBuilder().apply {
                                    streamName().isEqualTo(stream)
                                    direction().isEqualTo(direction)
                                    modifyFilterBuilderTimestamps(request)
                                    if (request.searchDirection == BEFORE) {
                                        order(Order.REVERSE)
                                    }
                                    subSink.limit?.let { limit(max(it, 0)) }
                                }.build()
                                val time = measureTimeMillis {
                                    cradleMsgExtractor.getMessages(filter, subSink, dataMeasurement)
                                }
                                logger.info { "Loaded ${subSink.loadedData} messages from DB $time ms" }
                            }
                        }
                    }

                } catch (e: Exception) {
                    logger.error(e) { "error getting messages" }
                    sink.onError(e)
                }
            }
        }
    }

    fun loadOneMessage(request: GetMessageRequest, requestContext: MessageResponseHandler, dataMeasurement: DataMeasurement) {
        threadPool.execute {
            RootMessagesDataSink(
                requestContext,
                if (request.onlyRaw) {
                    RawStoredMessageHandler(requestContext)
                } else {
                    ParsedStoredMessageHandler(requestContext, decoder, dataMeasurement, configuration.batchSize)
                }
            ).use { sink ->
                try {
                    cradleMsgExtractor.getMessage(StoredMessageId.fromString(request.msgId), sink, dataMeasurement);
                } catch (e: Exception) {
                    logger.error(e) { "error getting messages" }
                    sink.onError(e)
                }
            }
        }
    }

    private fun Set<ResponseFormat>?.hasRowOnly(): Boolean {
        return this != null && size == 1 && contains(ResponseFormat.BASE_64)
    }
}

private class RootMessagesDataSink(
    private val messageHandler: MessageResponseHandler,
    handler: ResponseHandler<StoredMessage>,
    limit: Int? = null,
) : MessagesDataSink(handler, limit) {
    override fun completed() {
        handler.complete()
        messageHandler.dataLoaded()
    }

    fun subSink(alias: String, direction: Direction): MessagesDataSink {
        messageHandler.registerSession(alias, direction)
        return SubMessagesDataSink(handler, limit?.minus(loadedData)?.toInt()) {
            loadedData += it
        }
    }
}

private class SubMessagesDataSink(
    handler: ResponseHandler<StoredMessage>,
    limit: Int? = null,
    private val onComplete: (loadedMessages: Long) -> Unit,
) : MessagesDataSink(handler, limit) {
    override fun completed() {
        handler.complete()
        onComplete(loadedData)
    }
}

private abstract class MessagesDataSink(
    override val handler: ResponseHandler<StoredMessage>,
    limit: Int? = null,
) : AbstractDataSink<StoredMessage>(handler, limit) {

    override fun onNext(listData: Collection<StoredMessage>) {
        listData.forEach(::onNext)
    }

    override fun onNext(data: StoredMessage) {
        if (limit == null || loadedData < limit) {
            loadedData++
            handler.handleNext(data)
        }
    }
}

private class ParsedStoredMessageHandler(
    private val handler: MessageResponseHandler,
    private val decoder: Decoder,
    private val measurement: DataMeasurement,
    private val batchSize: Int,
) : ResponseHandler<StoredMessage> {
    private val batch: MessageGroupBatch.Builder = MessageGroupBatch.newBuilder()
    private val details: MutableList<RequestedMessageDetails> = arrayListOf()
    override val isAlive: Boolean
        get() = handler.isAlive

    override fun complete() {
        processBatch(details)
    }

    override fun writeErrorMessage(text: String) {
        handler.writeErrorMessage(text)
    }

    override fun writeErrorMessage(error: Throwable) {
        handler.writeErrorMessage(error)
    }

    override fun handleNext(data: StoredMessage) {
        val step = measurement.start("decoding")
        val detail = RequestedMessageDetails(data) {
            step.stop()
            handler.requestReceived()
        }
        details += detail
        handler.handleNext(detail)
        if (details.size >= batchSize) {
            processBatch(details)
        }
    }

    private fun processBatch(details: MutableList<RequestedMessageDetails>) {
        if (details.isEmpty()) {
            return
        }
        handler.checkAndWaitForRequestLimit(details.size)
        decoder.sendBatchMessage(batch, details, details.first().storedMessage.streamName)
        details.clear()
        batch.clear()
    }
}

private class RawStoredMessageHandler(
    private val handler: MessageResponseHandler,
) : ResponseHandler<StoredMessage> {
    override val isAlive: Boolean
        get() = handler.isAlive

    override fun complete() {
    }

    override fun writeErrorMessage(text: String) {
        handler.writeErrorMessage(text)
    }

    override fun writeErrorMessage(error: Throwable) {
        handler.writeErrorMessage(error)
    }

    override fun handleNext(data: StoredMessage) {
        handler.handleNext(RequestedMessageDetails(data))
    }

}
