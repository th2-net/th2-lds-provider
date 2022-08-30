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
import com.exactpro.cradle.messages.StoredMessageFilterBuilder
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.lwdataprovider.MessageRequestContext
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import com.exactpro.th2.lwdataprovider.db.CradleGroupRequest
import com.exactpro.th2.lwdataprovider.db.CradleMessageExtractor
import com.exactpro.th2.lwdataprovider.entities.requests.GetMessageRequest
import com.exactpro.th2.lwdataprovider.entities.requests.MessagesGroupRequest
import com.exactpro.th2.lwdataprovider.entities.requests.SseMessageSearchRequest
import mu.KotlinLogging
import java.time.Instant
import java.util.concurrent.ExecutorService
import kotlin.math.max

class SearchMessagesHandler(
    private val cradleMsgExtractor: CradleMessageExtractor,
    private val threadPool: ExecutorService
) {
    companion object {
        private val logger = KotlinLogging.logger { }
    }

    fun extractStreamNames(): Collection<String> {
        logger.info { "Getting stream names" }
        return cradleMsgExtractor.getStreams();
    }
    
    fun loadMessages(request: SseMessageSearchRequest, requestContext: MessageRequestContext, configuration: Configuration) {
        
        if (request.stream == null && request.resumeFromIdsList.isNullOrEmpty()) {
            return;
        }

        threadPool.execute {
            try {

                var limitReached = false
                if (!request.resumeFromIdsList.isNullOrEmpty()) {
                    request.resumeFromIdsList.forEach { resumeFromId ->
                        requestContext.streamInfo.registerSession(resumeFromId.streamName, resumeFromId.direction)
                        if (limitReached)
                            return@forEach;
                        if (!requestContext.contextAlive)
                            return@execute;
                        loadByResumeId(resumeFromId, request, requestContext, configuration)
                        limitReached = requestContext.limitReached(request)
                    }
                } else {
                    request.stream?.forEach { (stream, direction) ->
                        requestContext.streamInfo.registerSession(stream, direction)
                        if (limitReached)
                            return@forEach;
                        if (!requestContext.contextAlive)
                            return@execute;
                        loadByStream(stream, direction, request, requestContext, configuration)

                        limitReached = requestContext.limitReached(request)
                    }
                }

                if (request.keepOpen && !limitReached) {
                    val lastTimestamp = checkNotNull(request.endTimestamp) { "end timestamp is null" }
                    val order = orderFrom(request)
                    val allLoaded = hashSetOf<Stream>()
                    do {
                        val continuePulling = pullUpdates(request, order, lastTimestamp, requestContext, configuration, allLoaded)
                    } while (continuePulling)
                }

                requestContext.allDataLoadedFromCradle()
                if (requestContext.requestedMessages.isEmpty()) {
                    requestContext.addStreamInfo()
                    requestContext.finishStream()
                }
            } catch (e: Exception) {
                logger.error("Error getting messages", e)
                requestContext.writeErrorMessage(e.message?:"")
                requestContext.finishStream()
            }
        }
    }

    fun loadOneMessage(request: GetMessageRequest, requestContext: MessageRequestContext) {

        threadPool.execute {
            try {
                cradleMsgExtractor.getMessage(StoredMessageId.fromString(request.msgId), request.onlyRaw, requestContext);
                requestContext.allDataLoadedFromCradle()
                if (requestContext.requestedMessages.isEmpty()) {
                    requestContext.finishStream()
                }
            } catch (e: Exception) {
                logger.error("Error getting messages", e)
                requestContext.writeErrorMessage(e.message?:"")
                requestContext.finishStream()
            }
        }
    }

    fun loadMessageGroups(request: MessagesGroupRequest, requestContext: MessageRequestContext) {
        if (request.groups.isEmpty()) {
            requestContext.finishStream()
        }

        threadPool.execute {
            try {
                val parameters = CradleGroupRequest(request.startTimestamp, request.endTimestamp, request.sort, request.rawOnly)
                request.groups.forEach { group ->
                    logger.debug { "Executing request for group $group" }
                    cradleMsgExtractor.getMessagesGroup(
                        group,
                        parameters,
                        requestContext
                    )
                    logger.debug { "Executing of request for group $group has been finished" }
                }

                if (request.keepOpen) {
                    val lastTimestamp: Instant = request.endTimestamp
                    val allGroupLoaded = hashSetOf<String>()
                    do {
                        val keepPulling = pullUpdates(request, lastTimestamp, requestContext, parameters, allGroupLoaded)
                    } while (keepPulling)
                }

                requestContext.allDataLoadedFromCradle()
                if (requestContext.requestedMessages.isEmpty()) {
                    requestContext.addStreamInfo()
                    requestContext.finishStream()
                }
            } catch (ex: Exception) {
                logger.error("Error getting messages group", ex)
                requestContext.writeErrorMessage(ex.message ?: "")
                requestContext.finishStream()
            }
        }
    }

    private fun pullUpdates(
        request: MessagesGroupRequest,
        lastTimestamp: Instant,
        requestContext: MessageRequestContext,
        parameters: CradleGroupRequest,
        allLoaded: MutableSet<String>,
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
            val lastGroupTimestamp = requestContext.streamInfo.lastTimestampForGroup(group)
                .coerceAtLeast(request.startTimestamp)
            val newParameters = if (lastGroupTimestamp == request.startTimestamp) {
                parameters
            } else {
                val lastIdByStream: Map<Pair<String, Direction>, StoredMessageId> = requestContext.streamInfo.lastIDsForGroup(group)
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
            cradleMsgExtractor.getMessagesGroup(group, newParameters, requestContext)
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
        requestContext: MessageRequestContext,
        configuration: Configuration,
        allLoaded: MutableSet<Stream>,
    ): Boolean {
        var limitReached = false
        var allDataLoaded = true

        val lastReceivedIDs: List<StoredMessageId> = requestContext.streamInfo.lastIDs
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
                loadByStream(messageId.streamName, messageId.direction, request, requestContext, configuration)
            } else {
                loadByResumeId(messageId, request, requestContext, configuration)
            }

            allDataLoaded = allDataLoaded and hasDataOutside
            limitReached = requestContext.limitReached(request)
        }
        return !allDataLoaded && !limitReached
    }

    private fun hasDataOutsideRequestRange(order: Order, it: StoredMessageId, lastTimestamp: Instant): Boolean = when (order) {
        Order.DIRECT -> cradleMsgExtractor.hasMessagesAfter(it, lastTimestamp)
        Order.REVERSE -> cradleMsgExtractor.hasMessagesBefore(it, lastTimestamp)
    }

    private fun MessageRequestContext.limitReached(request: SseMessageSearchRequest): Boolean =
        request.resultCountLimit != null && request.resultCountLimit <= loadedMessages

    private fun loadByResumeId(
        resumeFromId: StoredMessageId,
        request: SseMessageSearchRequest,
        requestContext: MessageRequestContext,
        configuration: Configuration,
    ) {
        val filter = StoredMessageFilterBuilder().apply {
            streamName().isEqualTo(resumeFromId.streamName)
            direction().isEqualTo(resumeFromId.direction)
            val order = orderFrom(request)
            order(order)
            indexFilter(request, resumeFromId)
            timestampsFilter(order, request)
            limitFilter(request, requestContext)
        }.build()

        val responseFormats = request.responseFormats ?: configuration.responseFormats
        if (request.onlyRaw || (responseFormats.contains("BASE_64") && responseFormats.size == 1)) {
            cradleMsgExtractor.getRawMessages(filter, requestContext)
        } else {
            cradleMsgExtractor.getMessages(filter, requestContext, responseFormats)
        }
    }

    private fun loadByStream(
        stream: String,
        direction: Direction,
        request: SseMessageSearchRequest,
        requestContext: MessageRequestContext,
        configuration: Configuration,
    ) {
        val order = orderFrom(request)
        val filter = StoredMessageFilterBuilder().apply {
            streamName().isEqualTo(stream)
            direction().isEqualTo(direction)
            order(order)
            timestampsFilter(order, request)
            limitFilter(request, requestContext)
        }.build()

        val responseFormats = request.responseFormats ?: configuration.responseFormats
        if (request.onlyRaw || (responseFormats.contains("BASE_64") && responseFormats.size == 1)) {
            cradleMsgExtractor.getRawMessages(filter, requestContext)
        } else {
            cradleMsgExtractor.getMessages(filter, requestContext, responseFormats)
        }
    }

    private fun StoredMessageFilterBuilder.limitFilter(
        request: SseMessageSearchRequest,
        requestContext: MessageRequestContext,
    ) {
        request.resultCountLimit?.let { limit(max(it - requestContext.loadedMessages, 0)) }
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
            request.endTimestamp?.let { timestampTo().isLessThan(it) }
        } else {
            request.startTimestamp?.let { timestampTo().isLessThanOrEqualTo(it) }
            request.endTimestamp?.let { timestampFrom().isGreaterThan(it) }
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

    
