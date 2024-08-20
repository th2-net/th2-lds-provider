/*
 * Copyright 2023-2024 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.dataprovider.lw.grpc.TimeRelation
import io.github.oshai.kotlinlogging.KotlinLogging
import io.grpc.Context
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
) {
    init {
        require(!searchStep.isZero) {
            "'searchStep' can't be zero"
        }
        require(!searchStep.isNegative) {
            "'searchStep' can't be negative"
        }
    }

    /**
     * Searches the first message in [BASE64_FORMAT] format matched by filter. Execute search from [Instant.now] to previous time
     * @param sessionGroup is parameter for request. Searcher uses [sessionAlias] value when [sessionGroup] is null
     * @param searchInterval for searching. Searcher stops to send requests when the next `from` time is before than search start time minus [searchInterval]
     */
    fun findLastOrNull(
        book: String,
        sessionGroup: String?,
        sessionAlias: String,
        direction: Direction,
        searchInterval: Duration,
        filter: Function<MessageSearchResponse, Boolean>
    ): MessageSearchResponse? {
        require(book.isNotBlank()) {
            "'book' can't be blank"
        }
        require(sessionGroup == null || sessionGroup.isNotBlank()) {
            "'sessionGroup' can't be blank"
        }
        require(sessionAlias.isNotBlank()) {
            "'sessionAlias' can't be blank"
        }
        require(!searchInterval.isZero) {
            "'searchInterval' can't be zero"
        }
        require(!searchInterval.isNegative) {
            "'searchInterval' can't be negative"
        }

        val now = Instant.now()
        val end = now.minus(searchInterval)

        val requestBuilder = createRequestBuilder(
            book = book,
            sessionGroup = sessionGroup ?: sessionAlias,
            sessionAlias = sessionAlias,
            direction = direction
        )
        K_LOGGER.info { "Search the last or null, base request: ${requestBuilder.toJson()}, start time: $now, end time: $end" }

        return withCancellation {
            sequence<MessageSearchResponse> {
                var from: Instant = now
                var to: Instant = from.minus(searchStep).run { if (this < end) end else this }

                while (from != to) {
                    service.searchMessageGroups(
                        requestBuilder.apply {
                            startTimestamp = from.toTimestamp()
                            endTimestamp = to.toTimestamp()
                        }.build().also {
                            K_LOGGER.debug { "Request from: $from, to: $to" }
                        }
                    ).forEach {
                        K_LOGGER.trace { "Received message: ${it.toJson()}" }
                        yield(it)
                    }

                    from = to
                    to = from.minus(searchStep).run { if (this < end) end else this }
                }
            }.firstOrNull(filter::apply)
        }
    }

    /**
     * Searches the first message in [BASE64_FORMAT] format matched by filter. Execute search from [Instant.now] to previous time
     * @see [findLastOrNull]
     */
    fun findLastOrNull(
        book: String,
        sessionAlias: String,
        direction: Direction,
        searchInterval: Duration,
        filter: Function<MessageSearchResponse, Boolean>
    ): MessageSearchResponse? = findLastOrNull(book, sessionAlias, sessionAlias, direction, searchInterval, filter)

    fun withSearchStep(searchStep: Duration) = MessageSearcher(service, searchStep)

    private fun createRequestBuilder(
        book: String,
        sessionGroup: String,
        sessionAlias: String,
        direction: Direction,
    ) = MessageGroupsSearchRequest.newBuilder().apply {
        bookIdBuilder.name = book
        addMessageGroup(MessageGroupsSearchRequest.Group.newBuilder().setName(sessionGroup))
        addStreamBuilder().apply {
            this.name = sessionAlias
            this.direction = direction
        }
        addResponseFormats(BASE64_FORMAT)
        searchDirection = TimeRelation.PREVIOUS
    }

    companion object {
        private val K_LOGGER = KotlinLogging.logger { }
        private const val BASE64_FORMAT = "BASE_64"

        @JvmField
        val DEFAULT_SEARCH_STEP: Duration = Duration.ofDays(1)

        @JvmOverloads
        @JvmStatic
        fun create(service: DataProviderService, searchStep: Duration = DEFAULT_SEARCH_STEP) =
            MessageSearcher(service, searchStep)

        private fun <T> withCancellation(code: () -> T): T {
            return Context.current().withCancellation().use { context ->
                context.call { code() }
            }
        }
    }
}