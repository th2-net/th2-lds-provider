/*******************************************************************************
 * Copyright 2021-2023 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.BookId
import com.exactpro.cradle.Direction
import com.exactpro.cradle.Order
import com.exactpro.cradle.messages.GroupedMessageFilter
import com.exactpro.cradle.messages.MessageFilterBuilder
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.lwdataprovider.Decoder
import com.exactpro.th2.lwdataprovider.ProviderStreamInfo
import com.exactpro.th2.lwdataprovider.RequestedMessageDetails
import com.exactpro.th2.lwdataprovider.ResponseHandler
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import com.exactpro.th2.lwdataprovider.db.CradleGroupRequest
import com.exactpro.th2.lwdataprovider.db.CradleMessageExtractor
import com.exactpro.th2.lwdataprovider.db.DataMeasurement
import com.exactpro.th2.lwdataprovider.entities.internal.ResponseFormat
import com.exactpro.th2.lwdataprovider.entities.requests.GetGroupMessageRequest
import com.exactpro.th2.lwdataprovider.entities.requests.GetMessageRequest
import com.exactpro.th2.lwdataprovider.entities.requests.MessagesGroupRequest
import com.exactpro.th2.lwdataprovider.entities.requests.SearchDirection
import com.exactpro.th2.lwdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.lwdataprovider.handlers.util.BookGroup
import com.exactpro.th2.lwdataprovider.handlers.util.GroupParametersHolder
import com.exactpro.th2.lwdataprovider.handlers.util.computeNewParametersForGroupRequest
import com.exactpro.th2.lwdataprovider.handlers.util.modifyFilterBuilderTimestamps
import com.exactpro.th2.lwdataprovider.toReportId
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

    fun extractGroups(bookId: BookId): Set<String> = cradleMsgExtractor.getGroups(bookId)

    fun extractStreamNames(bookId: BookId): Collection<String> {
        logger.info { "Getting stream names" }
        return cradleMsgExtractor.getStreams(bookId)
    }

    fun loadMessages(request: SseMessageSearchRequest, requestContext: MessageResponseHandler, dataMeasurement: DataMeasurement) {

        if (request.stream.isNullOrEmpty() && request.resumeFromIdsList.isNullOrEmpty()) {
            return
        }

        threadPool.execute {
            val rootSink = RootMessagesDataSink(
                requestContext,
                if (request.responseFormats.hasRowOnly()) {
                    RawStoredMessageHandler(requestContext)
                } else {
                    if (configuration.useTransportMode) {
                        TransportParsedStoredMessageHandler(requestContext, decoder, dataMeasurement, configuration.batchSize)
                    } else {
                        ProtoParsedStoredMessageHandler(requestContext, decoder, dataMeasurement, configuration.batchSize)
                    }
                },
                limit = request.resultCountLimit
            )
            try {
                rootSink.use { sink ->

                    if (!request.resumeFromIdsList.isNullOrEmpty()) {
                        request.resumeFromIdsList.forEach { resumeFromId ->
                            sink.canceled?.apply {
                                logger.info { "loading canceled: $message" }
                                return@use
                            }
                            loadByResumeId(resumeFromId, request, sink)
                        }
                    } else {
                        request.stream?.forEach { (stream, direction) ->
                            sink.canceled?.apply {
                                logger.info { "loading canceled: $message" }
                                return@use
                            }

                            loadByStream(stream, direction, request, sink)
                        }
                    }

                    if (request.keepOpen) {
                        sink.canceled?.apply {
                            logger.info { "request canceled: $message" }
                            return@use
                        }
                        val order = orderFrom(request)
                        val allLoaded = hashSetOf<Stream>()
                        do {
                            val continuePulling = pullUpdates(request, order, sink, allLoaded, dataMeasurement)
                        } while (continuePulling)
                    }
                }
            } catch (e: Exception) {
                logger.error(e) { "error getting messages" }
                rootSink.onError(e)
            }
        }
    }

    fun loadOneMessage(request: GetMessageRequest, requestContext: MessageResponseHandler, dataMeasurement: DataMeasurement) {
        threadPool.execute {
            val rootSink = RootMessagesDataSink(
                requestContext,
                if (request.onlyRaw) {
                    RawStoredMessageHandler(requestContext)
                } else {
                    if (configuration.useTransportMode) {
                        TransportParsedStoredMessageHandler(requestContext, decoder, dataMeasurement, configuration.batchSize)
                    } else {
                        ProtoParsedStoredMessageHandler(requestContext, decoder, dataMeasurement, configuration.batchSize)
                    }
                }
            )
            try {
                rootSink.use { sink ->
                    cradleMsgExtractor.getMessage(request.msgId, sink)
                }
            } catch (e: Exception) {
                logger.error(e) { "error getting messages" }
                rootSink.onError(e, request.msgId.toReportId())
            }
        }
    }

    fun loadOneMessageByGroup(request: GetGroupMessageRequest, requestContext: MessageResponseHandler, dataMeasurement: DataMeasurement) {
        threadPool.execute {
            val rootSink = RootMessagesDataSink(
                requestContext,
                if (request.rawOnly) {
                    RawStoredMessageHandler(requestContext)
                } else {
                    if (configuration.useTransportMode) {
                        TransportParsedStoredMessageHandler(requestContext, decoder, dataMeasurement, configuration.batchSize)
                    } else {
                        ProtoParsedStoredMessageHandler(requestContext, decoder, dataMeasurement, configuration.batchSize)
                    }
                }
            )
            try {
                rootSink.use { sink ->
                    cradleMsgExtractor.getMessage(request.group, request.messageId, sink)
                }
            } catch (e: Exception) {
                logger.error(e) { "error getting messages" }
                rootSink.onError(e, request.messageId.toReportId())
            }
        }
    }

    fun loadMessageGroups(request: MessagesGroupRequest, requestContext: MessageResponseHandler, dataMeasurement: DataMeasurement) {
        if (request.groups.isEmpty()) {
            requestContext.complete()
        }

        threadPool.execute {
            logger.info { "Executing group request $request" }
            val rootSink = RootMessagesDataSink(
                requestContext,
                if (request.responseFormats.hasRowOnly()) {
                    RawStoredMessageHandler(requestContext)
                } else {
                    if (configuration.useTransportMode) {
                        TransportParsedStoredMessageHandler(requestContext, decoder, dataMeasurement, configuration.batchSize)
                    } else {
                        ProtoParsedStoredMessageHandler(requestContext, decoder, dataMeasurement, configuration.batchSize)
                    }
                },
                markerAsGroup = true,
                limit = request.limit,
            )
            try {
                rootSink.use { sink ->

                    val parameters = CradleGroupRequest(
                        sort = request.sort,
                        preFilter = createInitialPrefilter(request),
                    )
                    request.groups.forEach { group ->
                        val filter = GroupedMessageFilter.builder()
                            .groupName(group)
                            .bookId(request.bookId)
                            .timestampFrom().isGreaterThanOrEqualTo(request.startTimestamp)
                            .timestampTo().isLessThan(request.endTimestamp)
                            .build()
                        logger.debug { "Executing request for group $group" }
                        cradleMsgExtractor.getMessagesGroup(filter, parameters, sink)
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
                            val keepPulling = pullUpdates(request, lastTimestamp, sink, parameters, allGroupLoaded)
                            sink.canceled?.apply {
                                logger.info { "request canceled: $message" }
                                return@use
                            }
                        } while (keepPulling)
                    }
                }
            } catch (ex: Exception) {
                logger.error("Error getting messages group", ex)
                rootSink.onError(ex)
            }
        }
    }

    private fun createInitialPrefilter(request: MessagesGroupRequest): ((StoredMessage) -> Boolean)? {
        if (request.includeStreams.isEmpty()) {
            return null
        }
        val includeMap: Map<String, Set<Direction>> = request.includeStreams
            .groupingBy { it.sessionAlias }
            .aggregate { key, acc: MutableSet<Direction>?, el, first ->
                el.direction.let {
                    if (first) {
                        hashSetOf(it)
                    } else {
                        requireNotNull(acc) { "accumulator is null for $key" }.apply { add(it) }
                    }
                }
            }

        return { msg ->
            includeMap[msg.sessionAlias]?.contains(msg.direction) ?: false
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
    ): Boolean {
        val parametersByBookId: Map<BookGroup, GroupParametersHolder> = computeNewParametersForGroupRequest(
            mapOf(request.bookId to request.groups),
            request.startTimestamp,
            lastTimestamp,
            parameters,
            allLoaded,
            sink.streamInfo,
            cradleMsgExtractor
        )
        parametersByBookId.forEach { (bookGroup, params) ->
            val (newStart, reqParams) = params
            val filter = GroupedMessageFilter.builder()
                .groupName(bookGroup.group)
                .bookId(bookGroup.bookId)
                .timestampFrom().isGreaterThanOrEqualTo(newStart)
                .timestampTo().isLessThan(request.endTimestamp)
                .build()
            cradleMsgExtractor.getMessagesGroup(filter, reqParams, sink)
            logger.info { "Data has been loaded for group $bookGroup" }
        }
        return allLoaded.size != request.groups.size
    }

    private data class Stream(val name: String, val direction: Direction)

    private fun pullUpdates(
        request: SseMessageSearchRequest,
        order: Order,
        sink: RootMessagesDataSink,
        allLoaded: MutableSet<Stream>,
        dataMeasurement: DataMeasurement,
    ): Boolean {
        var limitReached = false
        var allDataLoaded = true

        val lastReceivedIDs: List<StoredMessageId> = sink.streamInfo.lastIDs
        fun StoredMessageId.shortString(): String = "$sessionAlias:${direction.label}"
        fun StoredMessageId.toStream(): Stream = Stream(sessionAlias, direction)

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
            val hasDataOutside = hasDataOutsideRequestRange(order, messageId)
            if (hasDataOutside) {
                logger.info { "All data is loaded for ${messageId.shortString()}" }
                allLoaded += stream
            }
            if (messageId.sequence == 0L) {
                loadByStream(messageId.sessionAlias, messageId.direction, request, sink)
            } else {
                loadByResumeId(messageId, request, sink)
            }

            allDataLoaded = allDataLoaded and hasDataOutside
            limitReached = sink.limitReached()
        }
        return !allDataLoaded && !limitReached
    }

    private fun hasDataOutsideRequestRange(order: Order, it: StoredMessageId): Boolean = when (order) {
        Order.DIRECT -> cradleMsgExtractor.hasMessagesAfter(it)
        Order.REVERSE -> cradleMsgExtractor.hasMessagesBefore(it)
    }

    private fun RootMessagesDataSink.limitReached(): Boolean = canceled != null

    private fun loadByResumeId(
        resumeFromId: StoredMessageId,
        request: SseMessageSearchRequest,
        sink: RootMessagesDataSink,
    ) {
        sink.subSink(resumeFromId.sessionAlias, resumeFromId.direction).use { subSink ->
            val filter = MessageFilterBuilder().apply {
                bookId(request.bookId)
                sessionAlias(resumeFromId.sessionAlias)
                direction(resumeFromId.direction)
                val order = orderFrom(request)
                order(order)
                indexFilter(request, resumeFromId)
                modifyFilterBuilderTimestamps(request)
                limitFilter(sink)
            }.build()


            val time = measureTimeMillis {
                cradleMsgExtractor.getMessages(filter, subSink)
            }
            logger.info { "Loaded ${subSink.loadedData} messages from DB $time ms" }
        }
    }

    private fun loadByStream(
        stream: String,
        direction: Direction,
        request: SseMessageSearchRequest,
        sink: RootMessagesDataSink,
    ) {
        sink.subSink(stream, direction).use { subSink ->
            val filter = MessageFilterBuilder().apply {
                bookId(request.bookId)
                sessionAlias(stream)
                direction(direction)
                modifyFilterBuilderTimestamps(request)
                if (request.searchDirection == SearchDirection.previous) {
                    order(Order.REVERSE)
                }
                limitFilter(sink)
            }.build()
            val time = measureTimeMillis {
                cradleMsgExtractor.getMessages(filter, subSink)
            }
            logger.info { "Loaded ${subSink.loadedData} messages from DB $time ms" }
        }
    }

    private fun MessageFilterBuilder.limitFilter(
        sink: AbstractBasicDataSink,
    ) {
        sink.limit?.let { limit(max(it, 0)) }
    }

    private fun orderFrom(request: SseMessageSearchRequest): Order {
        val order = if (request.searchDirection == SearchDirection.next) {
            Order.DIRECT
        } else {
            Order.REVERSE
        }
        return order
    }

    private fun MessageFilterBuilder.indexFilter(
        request: SseMessageSearchRequest,
        resumeFromId: StoredMessageId,
    ) {
        if (request.searchDirection == SearchDirection.next) {
            sequence().isGreaterThanOrEqualTo(resumeFromId.sequence)
        } else {
            sequence().isLessThanOrEqualTo(resumeFromId.sequence)
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

    override fun onError(message: String, id: String?, batchId: String?) {
        super.onError(message, id, batchId)
        messageHandler.complete()
        messageHandler.cancel()
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

    override fun onNext(marker: String, listData: Collection<StoredMessage>) {
        var last: StoredMessage? = null
        for (data in listData) {
            val processed = process(marker, data)
            if (!processed) {
                break
            }
            last = data
        }
        last?.also { onNextData(marker, it) }
    }

    override fun onNext(marker: String, data: StoredMessage) {
        process(marker, data)
    }

    private fun process(marker: String, data: StoredMessage): Boolean {
        if (limit != null && loadedData >= limit) return false
        loadedData++
        onNextData(marker, data)
        handler.handleNext(data)
        return true
    }
}

private class RawStoredMessageHandler(
    private val handler: MessageResponseHandler,
) : ResponseHandler<StoredMessage> {
    override val isAlive: Boolean
        get() = handler.isAlive

    override fun complete() {
    }

    override fun writeErrorMessage(text: String, id: String?, batchId: String?) {
        handler.writeErrorMessage(text, id, batchId)
    }

    override fun writeErrorMessage(error: Throwable, id: String?, batchId: String?) {
        handler.writeErrorMessage(error, id, batchId)
    }

    override fun handleNext(data: StoredMessage) {
        handler.handleNext(RequestedMessageDetails(data))
    }

}
