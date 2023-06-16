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

package com.exactpro.th2.lwdataprovider.http

import com.exactpro.cradle.BookId
import com.exactpro.th2.lwdataprovider.EventType
import com.exactpro.th2.lwdataprovider.SseEvent
import com.exactpro.th2.lwdataprovider.SseEvent.Companion.DATA_CHARSET
import com.exactpro.th2.lwdataprovider.SseResponseBuilder
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import com.exactpro.th2.lwdataprovider.db.DataMeasurement
import com.exactpro.th2.lwdataprovider.entities.internal.ResponseFormat
import com.exactpro.th2.lwdataprovider.entities.requests.MessagesGroupRequest
import com.exactpro.th2.lwdataprovider.entities.requests.util.convertToMessageStreams
import com.exactpro.th2.lwdataprovider.entities.responses.ProviderMessage53
import com.exactpro.th2.lwdataprovider.handlers.SearchMessagesHandler
import com.exactpro.th2.lwdataprovider.http.util.listQueryParameters
import com.exactpro.th2.lwdataprovider.metrics.HttpWriteMetrics
import com.exactpro.th2.lwdataprovider.metrics.ResponseQueue
import com.exactpro.th2.lwdataprovider.workers.KeepAliveHandler
import io.javalin.Javalin
import io.javalin.http.Context
import io.javalin.http.Header
import io.javalin.http.HttpStatus
import io.javalin.http.queryParamAsClass
import io.javalin.openapi.HttpMethod
import io.javalin.openapi.OpenApi
import io.javalin.openapi.OpenApiContent
import io.javalin.openapi.OpenApiParam
import io.javalin.openapi.OpenApiResponse
import mu.KotlinLogging
import org.apache.commons.lang3.StringUtils
import java.time.Instant
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.Executor
import java.util.function.Supplier

class FileDownloadHandler(
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
                "(there is no order guaranties between groups). Messages for a group are not sorted by default. " +
                "Use $SORT_PARAMETER in order to sort messages for each group",
        queryParams = [
            OpenApiParam(
                GROUP_PARAM,
                type = Array<String>::class,
                required = true,
                description = "set of groups to request",
                isRepeatable = true,
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
                description = "enables message sorting in the request",
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
            groups = ctx.listQueryParameters(GROUP_PARAM)
                .check(List<*>::isNotEmpty, "EMPTY_COLLECTION")
                .check({ it.all(String::isNotBlank) }, "BLANK_GROUP")
                .get().toSet(),
            startTimestamp = ctx.queryParamAsClass<Instant>(START_TIMESTAMP_PARAM)
                .get(),
            endTimestamp = ctx.queryParamAsClass<Instant>(END_TIMESTAMP_PARAM)
                .get(),
            sort = ctx.queryParamAsClass<Boolean>(SORT_PARAMETER)
                .getOrDefault(false),
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
            processData(ctx, queue, handler)
            LOGGER.info { "Processing search sse messages group request finished" }
        }
    }

    private fun processData(
        ctx: Context,
        queue: ArrayBlockingQueue<Supplier<SseEvent>>,
        handler: HttpMessagesRequestHandler
    ) {
        val matchedPath = ctx.matchedPath()
        var dataSent = 0
        ctx.status(HttpStatus.OK)
            .contentType(JSON_STREAM_CONTENT_TYPE)
            .header(Header.TRANSFER_ENCODING, "chunked")
        val output = ctx.res().outputStream.buffered()
        val responseCharset = ctx.responseCharset()
        try {
            do {
                val nextEvent = queue.take()
                ResponseQueue.currentSize(matchedPath, queue.size)
                val sseEvent = nextEvent.get()
                when (sseEvent.event) {
                    EventType.KEEP_ALIVE -> output.flush()
                    EventType.CLOSE -> {
                        LOGGER.info { "Received close event" }
                        break
                    }
                    else -> {
                        LOGGER.debug { "Write event to output: ${StringUtils.abbreviate(sseEvent.data.toString(DATA_CHARSET), 100)}" }
                        output.write(sseEvent.data)
                        output.write('\n'.code)
                        dataSent++
                    }
                }
            } while (true)
        } catch (ex: Exception) {
            LOGGER.error(ex) { "cannot process next event" }
            handler.cancel()
            queue.clear()
        } finally {
            HttpWriteMetrics.messageSent(matchedPath, dataSent)
            runCatching { output.flush() }
                .onFailure { LOGGER.error(it) { "cannot flush the remaining data when processing is finished" } }
        }
    }

    companion object {
        private const val JSON_STREAM_CONTENT_TYPE = "application/stream+json"
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
        private val LOGGER = KotlinLogging.logger { }
        const val ROUTE_MESSAGES = "/download/messages"
    }
}