/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.lwdataprovider

import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.Direction.UNRECOGNIZED
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.dataprovider.lw.grpc.DataProviderService
import com.exactpro.th2.dataprovider.lw.grpc.MessageGroupsSearchRequest
import com.exactpro.th2.dataprovider.lw.grpc.MessageSearchResponse
import com.exactpro.th2.dataprovider.lw.grpc.MessageStream
import com.exactpro.th2.dataprovider.lw.grpc.TimeRelation
import io.grpc.Context
import mu.KotlinLogging
import java.time.Duration
import java.time.Instant
import java.util.function.Function

/**
 * This class provide ability to search messages via lw-data-provider gRPC service
 * @param searchStep interval for sending requests via [service]. Searcher uses this value to split search interval to smaller pieces to reduce volume of requested data via one time.
 */
class MessageSearcher private constructor(
    private val service: DataProviderService,
    private val searchStep: Duration = DEFAULT_SEARCH_STEP,
    private val defaultBook: String,
    private val defaultSessionGroup: String,
    private val defaultMessageStreams: Set<MessageStream>,
) {
    /**
     * Searches the first message in [BASE64_FORMAT] format matched by filter. Execute search from [Instant.now] to previous time
     * @param sessionGroup is parameter for request. Searcher uses [messageStreams] value when [sessionGroup] is null or blank
     * @param searchInterval for searching. Searcher stops to send requests when the next `from` time is before than search start time minus [searchInterval]
     */
    @JvmOverloads
    fun findLastOrNull(
        book: String = defaultBook,
        sessionGroup: String = defaultSessionGroup,
        messageStreams: Set<MessageStream> = defaultMessageStreams,
        searchInterval: Duration,
        filter: Function<MessageSearchResponse, Boolean>
    ): MessageSearchResponse? {
        require(book.isNotBlank()) {
            "'book' can't be blank"
        }
        require(sessionGroup.isNotBlank()) {
            "'sessionGroup' can't be blank"
        }
        messageStreams.forEach {
            require(it.name.isNotBlank()) {
                "'name' in ${it.toJson()} message stream can't be blank"
            }
            require(it.direction != UNRECOGNIZED) {
                "'direction' in ${it.toJson()} message stream can't be $UNRECOGNIZED"
            }
        }

        require(!searchInterval.isZero) {
            "'searchInterval' can't be zero"
        }
        require(!searchInterval.isNegative) {
            "'searchInterval' can't be negative"
        }

        val now = Instant.now()
        val end = now.minus(searchInterval)
        var from: Instant
        var to = now

        val requestBuilder = createRequestBuilder(
            book = book,
            sessionGroup = sessionGroup,
            messageStreams = messageStreams
        )
        K_LOGGER.info { "Search the last or null, base request: ${requestBuilder.toJson()}, start time: $now, end time: $end" }

        return withCancellation {
            generateSequence {
                if (to.isBefore(end)) {
                    return@generateSequence null
                }

                from = to
                to = from.minus(searchStep)

                service.searchMessageGroups(
                    requestBuilder.apply {
                        startTimestamp = from.toTimestamp()
                        endTimestamp = to.toTimestamp()
                    }.build().also {
                        K_LOGGER.debug { "Request from: $from, to: $to" }
                    }
                )
            }.flatMap(Iterator<MessageSearchResponse>::asSequence)
                .onEach { K_LOGGER.trace { "Message: ${it.toJson()}" } }
                .firstOrNull(filter::apply)
        }
    }

    @JvmOverloads
    fun with(
        requestInterval: Duration = DEFAULT_SEARCH_STEP,
        defaultBook: String = "",
        defaultSessionGroup: String = "",
        defaultMessageStreams: Set<MessageStream> = emptySet(),
    ) = MessageSearcher(service, requestInterval, defaultBook, defaultSessionGroup, defaultMessageStreams)

    fun withRequestInterval(requestInterval: Duration) =
        with(requestInterval, defaultBook, defaultSessionGroup, defaultMessageStreams)

    fun withBook(defaultBook: String) =
        with(searchStep, defaultBook, defaultSessionGroup, defaultMessageStreams)

    fun withSessionGroup(defaultSessionGroup: String) =
        with(searchStep, defaultBook, defaultSessionGroup, defaultMessageStreams)

    fun withMessageStreams(defaultMessageStreams: Set<MessageStream>) =
        with(searchStep, defaultBook, defaultSessionGroup, defaultMessageStreams)

    private fun createRequestBuilder(
        book: String,
        sessionGroup: String,
        messageStreams: Set<MessageStream>,
    ) = MessageGroupsSearchRequest.newBuilder().apply {
        bookIdBuilder.name = book
        addMessageGroup(MessageGroupsSearchRequest.Group.newBuilder().setName(sessionGroup))
        addAllStream(messageStreams)
        addResponseFormats(BASE64_FORMAT)
        searchDirection = TimeRelation.PREVIOUS
    }

    companion object {
        private val K_LOGGER = KotlinLogging.logger { }
        private const val BASE64_FORMAT = "BASE_64"

        @JvmField
        val DEFAULT_SEARCH_STEP: Duration = Duration.ofDays(1)

        @JvmOverloads
        fun create(
            service: DataProviderService,
            searchStep: Duration = DEFAULT_SEARCH_STEP,
            defaultBook: String = "",
            defaultSessionGroup: String = "",
            defaultMessageStreams: Set<MessageStream> = emptySet(),
        ) = MessageSearcher(
            service,
            searchStep,
            defaultBook,
            defaultSessionGroup,
            defaultMessageStreams,
        )

        fun create(
            sessionAlias: String,
            direction: Direction,
        ): MessageStream {
            require(sessionAlias.isNotBlank()) {
                "'sessionAlias' can't be blank"
            }
            require(direction != UNRECOGNIZED) {
                "'direction' can't be $UNRECOGNIZED"
            }

            return MessageStream.newBuilder()
                .setName(sessionAlias)
                .setDirection(direction)
                .build()
        }

        private fun <T> withCancellation(code: () -> T): T {
            return Context.current().withCancellation().use { context ->
                context.call { code() }
            }
        }
    }
}