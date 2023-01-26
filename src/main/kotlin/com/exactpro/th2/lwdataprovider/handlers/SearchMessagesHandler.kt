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

import com.exactpro.cradle.Order
import com.exactpro.cradle.TimeRelation.AFTER
import com.exactpro.cradle.messages.StoredMessageFilterBuilder
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.lwdataprovider.MessageRequestContext
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import com.exactpro.th2.lwdataprovider.db.CradleMessageExtractor
import com.exactpro.th2.lwdataprovider.entities.internal.ResponseFormat
import com.exactpro.th2.lwdataprovider.entities.requests.GetMessageRequest
import com.exactpro.th2.lwdataprovider.entities.requests.SseMessageSearchRequest
import mu.KotlinLogging
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

                var limitReached = false;
                if (!request.resumeFromIdsList.isNullOrEmpty()) {
                    request.resumeFromIdsList.forEach { resumeFromId ->
                        requestContext.streamInfo.registerSession(resumeFromId.streamName, resumeFromId.direction)
                        if (limitReached)
                            return@forEach;
                        if (!requestContext.contextAlive)
                            return@execute;
                        val filter = StoredMessageFilterBuilder().apply {
                            streamName().isEqualTo(resumeFromId.streamName)
                            direction().isEqualTo(resumeFromId.direction)
                            if (request.searchDirection == AFTER) {
                                index().isGreaterThanOrEqualTo(resumeFromId.index)
                            } else {
                                index().isLessThanOrEqualTo(resumeFromId.index)
                            }

                            request.startTimestamp?.let { timestampFrom().isGreaterThanOrEqualTo(it) }
                            request.endTimestamp?.let { timestampTo().isLessThan(it) }
                            request.resultCountLimit?.let { limit(max(it - requestContext.loadedMessages, 0)) }

                        }.build()

                        val responseFormats = request.responseFormats ?: configuration.defaultResponseFormats
                        if (isOnlyRaw(request, responseFormats)) {
                            cradleMsgExtractor.getRawMessages(filter, requestContext, responseFormats)
                        } else {
                            cradleMsgExtractor.getMessages(filter, requestContext, responseFormats)
                        }
                        limitReached = request.resultCountLimit != null && request.resultCountLimit <= requestContext.loadedMessages
                    }
                } else {
                    request.stream?.forEach { (stream, direction) ->
                        requestContext.streamInfo.registerSession(stream, direction)
                        if (limitReached)
                            return@forEach;
                        if (!requestContext.contextAlive)
                            return@execute;

                        val order = if(request.searchDirection == AFTER) {
                            Order.DIRECT
                        } else {
                            Order.REVERSE
                        }

                        val filter = StoredMessageFilterBuilder().apply {
                            streamName().isEqualTo(stream)
                            direction().isEqualTo(direction)
                            order(order)
                            if(order == Order.DIRECT) {
                                request.startTimestamp?.let { timestampFrom().isGreaterThanOrEqualTo(it) }
                                request.endTimestamp?.let { timestampTo().isLessThan(it) }
                            } else {
                                request.startTimestamp?.let { timestampTo().isLessThanOrEqualTo(it) }
                                request.endTimestamp?.let { timestampFrom().isGreaterThan(it) }
                            }
                            request.resultCountLimit?.let { limit(max(it - requestContext.loadedMessages, 0)) }
                        }.build()

                        val responseFormats = request.responseFormats ?: configuration.defaultResponseFormats
                        if (isOnlyRaw(request, responseFormats)) {
                            cradleMsgExtractor.getRawMessages(filter, requestContext, responseFormats)
                        } else {
                            cradleMsgExtractor.getMessages(filter, requestContext, responseFormats)
                        }

                        limitReached = request.resultCountLimit != null && request.resultCountLimit <= requestContext.loadedMessages
                    }
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

    private fun isOnlyRaw(request: SseMessageSearchRequest, responseFormats: List<ResponseFormat>) : Boolean{
        return request.onlyRaw || (responseFormats.contains(ResponseFormat.BASE_64) && responseFormats.size == 1)
    }
}

    
