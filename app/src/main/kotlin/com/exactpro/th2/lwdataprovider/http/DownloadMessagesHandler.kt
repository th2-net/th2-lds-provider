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

package com.exactpro.th2.lwdataprovider.http

import com.exactpro.cradle.BookId
import com.exactpro.th2.lwdataprovider.SseEvent
import com.exactpro.th2.lwdataprovider.SseResponseBuilder
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import com.exactpro.th2.lwdataprovider.db.DataMeasurement
import com.exactpro.th2.lwdataprovider.entities.internal.ResponseFormat
import com.exactpro.th2.lwdataprovider.entities.requests.MessagesGroupRequest
import com.exactpro.th2.lwdataprovider.entities.requests.SearchDirection
import com.exactpro.th2.lwdataprovider.entities.requests.util.convertToMessageStreams
import com.exactpro.th2.lwdataprovider.entities.responses.ProviderMessage53
import com.exactpro.th2.lwdataprovider.handlers.SearchMessagesHandler
import com.exactpro.th2.lwdataprovider.http.util.JSON_STREAM_CONTENT_TYPE
import com.exactpro.th2.lwdataprovider.http.util.writeJsonStream
import com.exactpro.th2.lwdataprovider.workers.KeepAliveHandler
import io.github.oshai.kotlinlogging.KotlinLogging
import io.javalin.Javalin
import io.javalin.http.Context
import io.javalin.http.queryParamAsClass
import io.javalin.http.queryParamsAsClass
import io.javalin.openapi.HttpMethod
import io.javalin.openapi.OpenApi
import io.javalin.openapi.OpenApiContent
import io.javalin.openapi.OpenApiParam
import io.javalin.openapi.OpenApiResponse
import java.time.Instant
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.Executor
import java.util.function.Supplier

class DownloadMessagesHandler(
    private val configuration: Configuration,
    private val convExecutor: Executor,
    private val sseResponseBuilder: SseResponseBuilder,
    private val keepAliveHandler: KeepAliveHandler,
    private val searchMessagesHandler: SearchMessagesHandler,
    private val dataMeasurement: DataMeasurement,
) : JavalinHandler {
    override fun setup(app: Javalin, context: JavalinContext) {
        app.get(ROUTE_MESSAGES, this::handleMessage)
    }

    @OpenApi(
        path = ROUTE_MESSAGES,
        description = "returns list of messages for specified groups. Each group will be requested one after another " +
                "(there is no order guaranties between groups). Messages for a group are not sorted by default. ",
        queryParams = [
            OpenApiParam(
                GROUP_PARAM,
                type = Array<String>::class,
                required = true,
                description = "set of groups to request",
            ),
            OpenApiParam(
                START_TIMESTAMP_PARAM,
                type = Long::class,
                description = "start timestamp for group search. Epoch time in milliseconds",
                required = true,
                example = HttpServer.TIME_EXAMPLE,
            ),
            OpenApiParam(
                END_TIMESTAMP_PARAM,
                type = Long::class,
                description = "end timestamp for group search. Epoch time in milliseconds",
                required = true,
                example = HttpServer.TIME_EXAMPLE,
            ),
            OpenApiParam(
                SORT_PARAMETER,
                type = Boolean::class,
                description = "enables message sorting in the request. " +
                        "Parameter is deprecated: grouped message batches are already sorted and haven't got overlapping",
                deprecated = true,
            ),
            OpenApiParam(
                RAW_ONLY_PARAMETER,
                type = Boolean::class,
                description = "only raw message will be returned in the response. " +
                        "Parameter is deprecated: use $RESPONSE_FORMAT with BASE_64 to achieve the same effect",
                deprecated = true,
            ),
            OpenApiParam(
                KEEP_OPEN_PARAMETER,
                type = Boolean::class,
                description = "enables pulling for updates until have not found a message outside the requested interval",
            ),
            OpenApiParam(
                BOOK_ID_PARAM,
                description = "book ID for requested groups",
                required = true,
                example = "bookId123",
            ),
            OpenApiParam(
                RESPONSE_FORMAT,
                type = Array<ResponseFormat>::class,
                description = "the format of the response"
            ),
            OpenApiParam(
                STREAM,
                type = Array<String>::class,
                description = "list of streams (optionally with direction) to include in the response",
            ),
            OpenApiParam(
                LIMIT,
                type = Int::class,
                description = "limit for messages in the response. No limit if not specified",
            ),
            OpenApiParam(
                SEARCH_DIRECTION,
                type = SearchDirection::class,
                description = "defines the order of the messages",
                example = "next",
            ),
        ],
        methods = [HttpMethod.GET],
        responses = [
            OpenApiResponse(
                status = "200",
                content = [
                    OpenApiContent(
                        from = ProviderMessage53::class,
                        mimeType = JSON_STREAM_CONTENT_TYPE,
                    )
                ]
            )
        ]
    )
    private fun handleMessage(ctx: Context) {
        val request = MessagesGroupRequest(
            groups = ctx.queryParamsAsClass<String>(GROUP_PARAM)
                .check(List<*>::isNotEmpty, "EMPTY_COLLECTION")
                .check({ it.all(String::isNotBlank) }, "BLANK_GROUP")
                .get().toSet(),
            startTimestamp = ctx.queryParamAsClass<Instant>(START_TIMESTAMP_PARAM)
                .get(),
            endTimestamp = ctx.queryParamAsClass<Instant>(END_TIMESTAMP_PARAM)
                .get(),
            keepOpen = ctx.queryParamAsClass<Boolean>(KEEP_OPEN_PARAMETER)
                .getOrDefault(false),
            bookId = ctx.queryParamAsClass<BookId>(BOOK_ID_PARAM).get(),
            responseFormats = ctx.queryParams(RESPONSE_FORMAT).takeIf(List<*>::isNotEmpty)
                ?.mapTo(hashSetOf(), ResponseFormat.Companion::fromString),
            includeStreams = ctx.queryParams(STREAM).takeIf(List<*>::isNotEmpty)
                ?.let(::convertToMessageStreams)
                ?.toSet() ?: emptySet(),
            limit = ctx.queryParamAsClass<Int>(LIMIT).allowNullable().check({
                it == null || it >= 0
            }, "NEGATIVE_LIMIT").get(),
            searchDirection = ctx.queryParamAsClass<SearchDirection>(SEARCH_DIRECTION)
                .getOrDefault(SearchDirection.next),
        )

        val queue = ArrayBlockingQueue<Supplier<SseEvent>>(configuration.responseQueueSize)
        val responseFormats: Set<ResponseFormat>? = request.responseFormats.let { formats ->
            if (ctx.queryParamAsClass<Boolean>(RAW_ONLY_PARAMETER).getOrDefault(false)) {
                formats?.let { it + ResponseFormat.BASE_64 } ?: setOf(ResponseFormat.BASE_64)
            } else {
                formats
            }
        }
        val handler = HttpMessagesRequestHandler(
            queue, sseResponseBuilder, convExecutor, dataMeasurement,
            maxMessagesPerRequest = configuration.bufferPerQuery,
            responseFormats = responseFormats ?: configuration.responseFormats
        )
        keepAliveHandler.addKeepAliveData(handler).use {
            searchMessagesHandler.loadMessageGroups(request, handler, dataMeasurement)
            writeJsonStream(ctx, queue, handler, dataMeasurement, LOGGER)
            LOGGER.info { "Processing download messages request finished" }
        }
    }

    companion object {
        private const val GROUP_PARAM = "group"
        private const val START_TIMESTAMP_PARAM = "startTimestamp"
        private const val END_TIMESTAMP_PARAM = "endTimestamp"
        private const val SORT_PARAMETER = "sort"
        private const val RAW_ONLY_PARAMETER = "onlyRaw"
        private const val KEEP_OPEN_PARAMETER = "keepOpen"
        private const val BOOK_ID_PARAM = "bookId"
        private const val RESPONSE_FORMAT = "responseFormat"
        private const val STREAM = "stream"
        private const val LIMIT = "limit"
        private const val SEARCH_DIRECTION = "searchDirection"
        private val LOGGER = KotlinLogging.logger { }
        const val ROUTE_MESSAGES = "/download/messages"
    }
}