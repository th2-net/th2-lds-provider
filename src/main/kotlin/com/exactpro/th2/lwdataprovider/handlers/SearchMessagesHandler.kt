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
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageFilterBuilder
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.lwdataprovider.Decoder
import com.exactpro.th2.lwdataprovider.ProviderStreamInfo
import com.exactpro.th2.lwdataprovider.RequestedMessageDetails
import com.exactpro.th2.lwdataprovider.ResponseHandler
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import com.exactpro.th2.lwdataprovider.db.CradleGroupRequest
import com.exactpro.th2.lwdataprovider.db.CradleMessageExtractor
import com.exactpro.th2.lwdataprovider.db.DataMeasurement
import com.exactpro.th2.lwdataprovider.entities.internal.ResponseFormat
import com.exactpro.th2.lwdataprovider.entities.requests.GetMessageRequest
import com.exactpro.th2.lwdataprovider.entities.requests.MessagesGroupRequest
import com.exactpro.th2.lwdataprovider.entities.requests.SseMessageSearchRequest
import mu.KotlinLogging
import java.time.Instant
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
                limit = request.resultCountLimit
            ).use { sink ->
                try {
                    if (!request.resumeFromIdsList.isNullOrEmpty()) {
                        request.resumeFromIdsList.forEach { resumeFromId ->
                            sink.canceled?.apply {
                                logger.info { "loading canceled: $message" }
                                return@use
                            }
                            loadByResumeId(resumeFromId, request, sink, dataMeasurement)
                        }
                    } else {
                        request.stream?.forEach { (stream, direction) ->
                            sink.canceled?.apply {
                                logger.info { "loading canceled: $message" }
                                return@use
                            }

                            loadByStream(stream, direction, request, sink, dataMeasurement)
                        }
                    }

                    if (request.keepOpen ) {
                        sink.canceled?.apply {
                            logger.info { "request canceled: $message" }
                            return@use
                        }
                        val lastTimestamp = checkNotNull(request.endTimestamp) { "end timestamp is null" }
                        val order = orderFrom(request)
                        val allLoaded = hashSetOf<Stream>()
                        do {
                            val continuePulling = pullUpdates(request, order, lastTimestamp, sink, allLoaded, dataMeasurement)
                        } while (continuePulling)
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

    fun loadMessageGroups(request: MessagesGroupRequest, requestContext: MessageResponseHandler, dataMeasurement: DataMeasurement) {
        if (request.groups.isEmpty()) {
            requestContext.complete()
        }

        threadPool.execute {
            RootMessagesDataSink(
                requestContext,
                if (request.rawOnly) {
                    RawStoredMessageHandler(requestContext)
                } else {
                    ParsedStoredMessageHandler(requestContext, decoder, dataMeasurement, configuration.batchSize)
                },
                markerAsGroup = true,
                limit = null,
            ).use { sink ->
                try {
                    val parameters = CradleGroupRequest(request.startTimestamp, request.endTimestamp, request.sort)
                    request.groups.forEach { group ->
                        logger.debug { "Executing request for group $group" }
                        cradleMsgExtractor.getMessagesGroup(group, parameters, sink, dataMeasurement)
                        logger.debug { "Executing of request for group $group has been finished" }
                    }

                    if (request.keepOpen) {
                        sink.canceled?.apply {
                            logger.info { "request canceled: $message" }
                            return@use
                        }
                        val lastTimestamp: Instant = request.endTimestamp
                        val allGroupLoaded = hashSetOf<String>()
                        do {
                            val keepPulling = pullUpdates(request, lastTimestamp, sink, parameters, allGroupLoaded, dataMeasurement)
                        } while (keepPulling)
                    }
                } catch (ex: Exception) {
                    logger.error("Error getting messages group", ex)
                    sink.onError(ex)
                }
            }
        }
    }

    private fun Set<ResponseFormat>?.hasRowOnly(): Boolean {
        return this != null && size == 1 && contains(ResponseFormat.BASE_64)
    }

    private fun pullUpdates(
        request: MessagesGroupRequest,
        lastTimestamp: Instant,
        sink: RootMessagesDataSink,
        parameters: CradleGroupRequest,
        allLoaded: MutableSet<String>,
        dataMeasurement: DataMeasurement,
    ): Boolean {
        var allDataLoaded = true
        request.groups.forEach { group ->
            if (group in allLoaded) {
                logger.trace { "Skip pulling data for group $group because all data is already loaded" }
                return@forEach
            }
            logger.info { "Pulling updates for group $group" }
            val hasDataOutsideRange = cradleMsgExtractor.hasMessagesInGroupAfter(group, lastTimestamp)
            if (hasDataOutsideRange) {
                logger.info { "All data in requested range is loaded for group $group" }
                allLoaded += group
            }
            logger.info { "Requesting additional data for group $group" }
            val lastGroupTimestamp = sink.streamInfo.lastTimestampForGroup(group)
                .coerceAtLeast(request.startTimestamp)
            val newParameters = if (lastGroupTimestamp == request.startTimestamp) {
                parameters
            } else {
                val lastIdByStream: Map<Pair<String, Direction>, StoredMessageId> = sink.streamInfo.lastIDsForGroup(group)
                    .associateBy { it.streamName to it.direction }
                parameters.copy(
                    start = lastGroupTimestamp,
                    preFilter = { msg ->
                        val stream = msg.run { streamName to direction }
                        lastIdByStream[stream]?.run {
                            msg.index > index
                        } ?: true
                    }
                )
            }
            cradleMsgExtractor.getMessagesGroup(group, newParameters, sink, dataMeasurement)
            logger.info { "Data has been loaded for group $group" }
            allDataLoaded = allDataLoaded and hasDataOutsideRange
        }
        return !allDataLoaded
    }

    private data class Stream(val name: String, val direction: Direction)
    private fun pullUpdates(
        request: SseMessageSearchRequest,
        order: Order,
        lastTimestamp: Instant,
        sink: RootMessagesDataSink,
        allLoaded: MutableSet<Stream>,
        dataMeasurement: DataMeasurement,
    ): Boolean {
        var limitReached = false
        var allDataLoaded = true

        val lastReceivedIDs: List<StoredMessageId> = sink.streamInfo.lastIDs
        fun StoredMessageId.shortString(): String = "$streamName:${direction.label}"
        fun StoredMessageId.toStream(): Stream = Stream(streamName, direction)

        lastReceivedIDs.forEach { messageId ->
            if (limitReached) {
                return false
            }
            val stream = messageId.toStream()
            if (stream in allLoaded) {
                logger.trace { "Skip pulling data for $stream because all data is loaded" }
                return@forEach
            }
            logger.info { "Pulling data for ${messageId.shortString()}" }
            val hasDataOutside = hasDataOutsideRequestRange(order, messageId, lastTimestamp)
            if (hasDataOutside) {
                logger.info { "All data is loaded for ${messageId.shortString()}" }
                allLoaded += stream
            }
            if (messageId.index == 0L) {
                loadByStream(messageId.streamName, messageId.direction, request, sink, dataMeasurement)
            } else {
                loadByResumeId(messageId, request, sink, dataMeasurement)
            }

            allDataLoaded = allDataLoaded and hasDataOutside
            limitReached = sink.limitReached()
        }
        return !allDataLoaded && !limitReached
    }

    private fun hasDataOutsideRequestRange(order: Order, it: StoredMessageId, lastTimestamp: Instant): Boolean = when (order) {
        Order.DIRECT -> cradleMsgExtractor.hasMessagesAfter(it, lastTimestamp)
        Order.REVERSE -> cradleMsgExtractor.hasMessagesBefore(it, lastTimestamp)
    }

    private fun RootMessagesDataSink.limitReached(): Boolean = canceled != null

    private fun loadByResumeId(
        resumeFromId: StoredMessageId,
        request: SseMessageSearchRequest,
        sink: RootMessagesDataSink,
        dataMeasurement: DataMeasurement,
    ) {
        sink.subSink(resumeFromId.streamName, resumeFromId.direction).use { subSink ->
            val filter = StoredMessageFilterBuilder().apply {
                streamName().isEqualTo(resumeFromId.streamName)
                direction().isEqualTo(resumeFromId.direction)
                val order = orderFrom(request)
                order(order)
                indexFilter(request, resumeFromId)
                timestampsFilter(order, request)
                limitFilter(sink)
            }.build()


            val time = measureTimeMillis {
                cradleMsgExtractor.getMessages(filter, subSink, dataMeasurement)
            }
            logger.info { "Loaded ${subSink.loadedData} messages from DB $time ms" }
        }
    }

    private fun loadByStream(
        stream: String,
        direction: Direction,
        request: SseMessageSearchRequest,
        sink: RootMessagesDataSink,
        dataMeasurement: DataMeasurement,
    ) {
        sink.subSink(stream, direction).use { subSink ->
            val order = orderFrom(request)
            val filter = StoredMessageFilterBuilder().apply {
                streamName().isEqualTo(stream)
                direction().isEqualTo(direction)
                order(order)
                timestampsFilter(order, request)
                limitFilter(sink)
            }.build()
            val time = measureTimeMillis {
                cradleMsgExtractor.getMessages(filter, subSink, dataMeasurement)
            }
            logger.info { "Loaded ${subSink.loadedData} messages from DB $time ms" }
        }
    }

    private fun StoredMessageFilterBuilder.limitFilter(
        sink: AbstractBasicDataSink,
    ) {
        sink.limit?.let { limit(max(it, 0)) }
    }

    private fun orderFrom(request: SseMessageSearchRequest): Order {
        val order = if (request.searchDirection == AFTER) {
            Order.DIRECT
        } else {
            Order.REVERSE
        }
        return order
    }

    private fun StoredMessageFilterBuilder.timestampsFilter(
        order: Order,
        request: SseMessageSearchRequest,
    ) {
        if (order == Order.DIRECT) {
            request.startTimestamp?.let { timestampFrom().isGreaterThanOrEqualTo(it) }
            request.endTimestamp.let { timestampTo().isLessThan(it) }
        } else {
            request.startTimestamp?.let { timestampTo().isLessThanOrEqualTo(it) }
            request.endTimestamp.let { timestampFrom().isGreaterThan(it) }
        }
    }

    private fun StoredMessageFilterBuilder.indexFilter(
        request: SseMessageSearchRequest,
        resumeFromId: StoredMessageId,
    ) {
        if (request.searchDirection == AFTER) {
            index().isGreaterThanOrEqualTo(resumeFromId.index)
        } else {
            index().isLessThanOrEqualTo(resumeFromId.index)
        }
    }
}

private class RootMessagesDataSink(
    private val messageHandler: MessageResponseHandler,
    handler: ResponseHandler<StoredMessage>,
    private val markerAsGroup: Boolean = false,
    limit: Int? = null,
) : MessagesDataSink(handler, limit) {

    val streamInfo: ProviderStreamInfo
        get() = messageHandler.streamInfo
    override fun completed() {
        handler.complete()
        messageHandler.dataLoaded()
    }

    fun subSink(alias: String, direction: Direction, group: String? = null): MessagesDataSink {
        messageHandler.registerSession(alias, direction, group)
        return SubMessagesDataSink(handler, this::onNextData, limit?.minus(loadedData)?.toInt()) {
            loadedData += it
        }
    }

    override fun onNextData(marker: String, data: StoredMessage) {
        streamInfo.registerMessage(data.id, data.timestamp, if (markerAsGroup) marker else null)
    }
}

private class SubMessagesDataSink(
    handler: ResponseHandler<StoredMessage>,
    private val onNextDataCall: (String, StoredMessage) -> Unit,
    limit: Int? = null,
    private val onComplete: (loadedMessages: Long) -> Unit,
) : MessagesDataSink(handler, limit) {
    override fun completed() {
        handler.complete()
        onComplete(loadedData)
    }

    override fun onNextData(marker: String, data: StoredMessage) = onNextDataCall(marker, data)
}

private abstract class MessagesDataSink(
    override val handler: ResponseHandler<StoredMessage>,
    limit: Int? = null,
) : AbstractMessageDataSink<String, StoredMessage>(handler, limit) {

    protected abstract fun onNextData(marker: String, data: StoredMessage)

    override fun onNext(marker: String, data: StoredMessage) {
        if (limit == null || loadedData < limit) {
            loadedData++
            onNextData(marker, data)
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
