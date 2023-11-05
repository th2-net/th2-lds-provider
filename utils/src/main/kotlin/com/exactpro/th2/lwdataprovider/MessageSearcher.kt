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
 * @param requestInterval interval for sending requests via [service]. Searcher uses this value to split search interval to smaller pieces to reduce volume of requested data via one time.
 */
class MessageSearcher @JvmOverloads constructor(
    private val service: DataProviderService,
    private val requestInterval: Duration = DEFAULT_REQUEST_INTERVAL,
    private val defaultBook: String = "",
    private val defaultSessionGroup: String? = null,
    private val defaultSessionAlias: String = "",
) {
    /**
     * Searches the first message in [BASE64_FORMAT] format matched by filter. Execute search from [Instant.now] to previous time
     * @param sessionGroup is parameter for request. Searcher uses [sessionAlias] value when [sessionGroup] is null or blank
     * @param interval for searching. Searcher stops to send requests when the next `from` time is before than search start time minus [interval]
     */
    fun searchLastOrNull(
        book: String = defaultBook,
        sessionGroup: String? = defaultSessionGroup,
        sessionAlias: String = defaultSessionAlias,
        direction: Direction,
        interval: Duration,
        filter: Function<MessageSearchResponse, Boolean>
    ): MessageSearchResponse? {
        require(book.isNotBlank()) {
            "'book' can't be blank"
        }
        require(sessionAlias.isNotBlank()) {
            "'sessionAlias' can't be blank"
        }
        require(!interval.isZero) {
            "'duration' can't be zero"
        }
        require(!interval.isNegative) {
            "'duration' can't be negative"
        }

        val now = Instant.now()
        val end = now.minus(interval)
        var from: Instant
        var to = now

        val requestBuilder = createRequestBuilder(
            book = book,
            sessionGroup = sessionGroup?.ifBlank { sessionAlias } ?: sessionAlias,
            sessionAlias = sessionAlias,
            direction = direction
        )
        K_LOGGER.info { "Search the last or null, base request: ${requestBuilder.toJson()}, start time: $now, end time: $end" }

        return withCancellation {
            generateSequence {
                if (to.isBefore(end)) {
                    return@generateSequence null
                }

                from = to
                to = from.minus(requestInterval)

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

    private fun createRequestBuilder(
        book: String,
        sessionGroup: String,
        sessionAlias: String,
        direction: Direction,
    ) = MessageGroupsSearchRequest.newBuilder().apply {
        bookIdBuilder.name = book
        addMessageGroup(MessageGroupsSearchRequest.Group.newBuilder().setName(sessionGroup))
        addStream(
            MessageStream.newBuilder()
                .setName(sessionAlias)
                .setDirection(direction)
        )
        addResponseFormats(BASE64_FORMAT)
        searchDirection = TimeRelation.PREVIOUS
    }

    companion object {
        private val K_LOGGER = KotlinLogging.logger { }
        private const val BASE64_FORMAT = "BASE_64"

        @JvmField
        val DEFAULT_REQUEST_INTERVAL: Duration = Duration.ofDays(1)

        @JvmOverloads
        fun MessageSearcher.with(
            requestInterval: Duration = DEFAULT_REQUEST_INTERVAL,
            defaultBook: String = "",
            defaultSessionGroup: String? = null,
            defaultSessionAlias: String = "",
        ) = MessageSearcher(service, requestInterval, defaultBook, defaultSessionGroup, defaultSessionAlias)

        fun MessageSearcher.withRequestInterval(requestInterval: Duration) =
            with(requestInterval, defaultBook, defaultSessionGroup, defaultSessionAlias)

        fun MessageSearcher.withBook(defaultBook: String) =
            with(requestInterval, defaultBook, defaultSessionGroup, defaultSessionAlias)

        fun MessageSearcher.withSessionGroup(defaultSessionGroup: String) =
            with(requestInterval, defaultBook, defaultSessionGroup, defaultSessionAlias)

        fun MessageSearcher.withSessionAlias(defaultSessionAlias: String) =
            with(requestInterval, defaultBook, defaultSessionGroup, defaultSessionAlias)

        private fun <T> withCancellation(code: () -> T): T {
            return Context.current().withCancellation().use { context ->
                context.call { code() }
            }
        }
    }
}