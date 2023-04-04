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

package com.exactpro.th2.lwdataprovider.http

import com.exactpro.cradle.BookId
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.lwdataprovider.SseEvent
import com.exactpro.th2.lwdataprovider.SseResponseBuilder
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import com.exactpro.th2.lwdataprovider.db.DataMeasurement
import com.exactpro.th2.lwdataprovider.entities.internal.ResponseFormat
import com.exactpro.th2.lwdataprovider.entities.requests.SearchDirection
import com.exactpro.th2.lwdataprovider.entities.requests.SseMessageSearchRequest
import com.exactpro.th2.lwdataprovider.entities.responses.ProviderMessage53
import com.exactpro.th2.lwdataprovider.handlers.SearchMessagesHandler
import com.exactpro.th2.lwdataprovider.http.JavalinHandler.Companion.customSse
import com.exactpro.th2.lwdataprovider.workers.KeepAliveHandler
import io.javalin.Javalin
import io.javalin.http.Context
import io.javalin.http.queryParamAsClass
import io.javalin.openapi.HttpMethod
import io.javalin.openapi.OpenApi
import io.javalin.openapi.OpenApiContent
import io.javalin.openapi.OpenApiParam
import io.javalin.openapi.OpenApiResponse
import mu.KotlinLogging
import java.time.Instant
import java.util.concurrent.ArrayBlockingQueue
import java.util.function.Supplier

private const val START_TIMESTAMP = "startTimestamp"

private const val END_TIMESTAMP = "endTimestamp"

private const val STREAM = "stream"

private const val SEARCH_DIRECTION = "searchDirection"

private const val MESSAGE_ID = "messageId"

private const val RESULT_COUNT_LIMIT = "resultCountLimit"

private const val KEEP_OPEN = "keepOpen"

private const val RESPONSE_FORMAT = "responseFormat"

private const val BOOK_ID = "bookId"

class GetMessagesServlet(
    private val configuration: Configuration,
    private val sseResponseBuilder: SseResponseBuilder,
    private val keepAliveHandler: KeepAliveHandler,
    private val searchMessagesHandler: SearchMessagesHandler,
    private val dataMeasurement: DataMeasurement,
) : AbstractSseRequestHandler() {

    companion object {
        const val ROUTE = "/search/sse/messages"
        private const val REQUEST_KEY = "sse.messages.request"
        private val logger = KotlinLogging.logger { }
    }

    override fun setup(app: Javalin, context: JavalinContext) {
        app.before(ROUTE) {
            it.attribute(REQUEST_KEY, createRequest(it))
        }
        app.customSse(ROUTE, this, context)
    }

    @OpenApi(
        methods = [HttpMethod.GET],
        path = ROUTE,
        description = "returns list of messages for specified streams (alias + direction). Messages are not sorted between stream. " +
                "Order is guarantied only within stream (alias + direction)",
        queryParams = [
            OpenApiParam(
                START_TIMESTAMP,
                type = Long::class,
                required = true,
                description = "start timestamp for search", example = HttpServer.TIME_EXAMPLE),
            OpenApiParam(
                END_TIMESTAMP,
                type = Long::class,
                description = "end timestamp for search", example = HttpServer.TIME_EXAMPLE),
            OpenApiParam(
                SEARCH_DIRECTION,
                type = SearchDirection::class,
                description = "defines the order of the messages", example = "next"),
            OpenApiParam(
                RESULT_COUNT_LIMIT,
                type = Int::class,
                description = "limit for messages in the response", example = "42"),
            OpenApiParam(
                BOOK_ID,
                type = String::class,
                required = true,
                description = "book ID for message", example = "book123"),
            OpenApiParam(
                MESSAGE_ID,
                type = Array<String>::class,
                description = "list of IDs to start request from. It must be specified if $STREAM parameter is not used",
                example = "book:session_alias:1:20221031130000000000000:1"),
            OpenApiParam(
                STREAM,
                type = Array<String>::class,
                description = "list of session aliases to request in format '<session_alias>[:<direction>]'. " +
                        "If the direction is not specified all known directions will be requested for session alias. " +
                        "Possible directions: 1 - incoming messages, 2 - outgoing messages. " +
                        "Parameter must be specified if $MESSAGE_ID parameter is not used",
                example = "session_alias:1",
            ),
            OpenApiParam(
                KEEP_OPEN,
                type = Boolean::class,
                description = "keeps pulling for new message until don't have one outside the requested range"),
            OpenApiParam(
                RESPONSE_FORMAT,
                type = Array<String>::class,
                description = "the format of the response"),
        ],
        responses = [
            OpenApiResponse(
                status = "200",
                content = [
                    OpenApiContent(from = ProviderMessage53::class, mimeType = "text/event-stream"),
                ],
                description = "message entity",
            )
        ]
    )
    override fun accept(sseClient: SseClient) {
        val ctx = sseClient.ctx()
        logger.info { "Received search sse event request with parameters: ${ctx.queryParamMap()}" }

        val request = checkNotNull(ctx.attribute<SseMessageSearchRequest>(REQUEST_KEY)) {
            "request was not created in before handler"
        }

        val queue = ArrayBlockingQueue<Supplier<SseEvent>>(configuration.responseQueueSize)
        val handler = HttpMessagesRequestHandler(queue, sseResponseBuilder, dataMeasurement, maxMessagesPerRequest = configuration.bufferPerQuery,
            responseFormats = request.responseFormats ?: configuration.responseFormats)
        sseClient.onClose(handler::cancel)
//        requestsDataMeasurement.start("messages_loading").use {
            keepAliveHandler.addKeepAliveData(handler).use {
                searchMessagesHandler.loadMessages(request, handler, dataMeasurement)

                sseClient.waitAndWrite(queue)
                logger.info { "Processing search sse messages request finished" }
            }
//        }
    }

    private fun createRequest(ctx: Context) = SseMessageSearchRequest(
        startTimestamp = ctx.queryParamAsClass<Instant>(START_TIMESTAMP)
            .allowNullable().get(),
        stream = ctx.listQueryParameters(STREAM).get()
            .takeIf(List<*>::isNotEmpty)
            ?.let(SseMessageSearchRequest::toStreams),
        searchDirection = ctx.queryParamAsClass<SearchDirection>(SEARCH_DIRECTION)
            .getOrDefault(SearchDirection.next),
        endTimestamp = ctx.queryParamAsClass<Instant>(END_TIMESTAMP).allowNullable().get(),
        resumeFromIdsList = ctx.listQueryParameters(MESSAGE_ID).get()
            .takeIf(List<*>::isNotEmpty)
            ?.map { StoredMessageId.fromString(it) },
        resultCountLimit = ctx.queryParamAsClass<Int>(RESULT_COUNT_LIMIT).allowNullable().get(),
        keepOpen = ctx.queryParamAsClass<Boolean>(KEEP_OPEN).getOrDefault(false),
        responseFormats = ctx.queryParams(RESPONSE_FORMAT).takeIf(List<*>::isNotEmpty)
            ?.mapTo(hashSetOf(), ResponseFormat.Companion::fromString),
        bookId = ctx.queryParamAsClass<BookId>(BOOK_ID).get(),
    )


}
