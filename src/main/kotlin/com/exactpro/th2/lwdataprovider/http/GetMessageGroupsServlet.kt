/*
 * Copyright 2022-2023 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.lwdataprovider.entities.responses.ProviderMessage53
import com.exactpro.th2.lwdataprovider.handlers.SearchMessagesHandler
import com.exactpro.th2.lwdataprovider.workers.KeepAliveHandler
import io.javalin.Javalin
import io.javalin.http.Context
import io.javalin.http.queryParamAsClass
import io.javalin.http.sse.SseClient
import io.javalin.openapi.HttpMethod
import io.javalin.openapi.OpenApi
import io.javalin.openapi.OpenApiContent
import io.javalin.openapi.OpenApiParam
import io.javalin.openapi.OpenApiResponse
import mu.KotlinLogging
import java.time.Instant
import java.util.concurrent.ArrayBlockingQueue
import java.util.function.Supplier

class GetMessageGroupsServlet(
    private val configuration: Configuration,
    private val sseResponseBuilder: SseResponseBuilder,
    private val keepAliveHandler: KeepAliveHandler,
    private val searchMessagesHandler: SearchMessagesHandler,
    private val dataMeasurement: DataMeasurement,
) : AbstractSseRequestHandler() {

    override fun setup(app: Javalin) {
        app.before(ROUTE) {
            it.attribute(REQUEST_KEY, createRequest(it))
        }
        app.sse(ROUTE, this)
    }

    @OpenApi(
        path = ROUTE,
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
                description = "only raw message will be returned in the response",
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
                type = Array<String>::class,
                description = "the format of the response"
            ),
        ],
        methods = [HttpMethod.GET],
        responses = [
            OpenApiResponse(
                status = "200",
                content = [
                    OpenApiContent(
                        from = ProviderMessage53::class,
                        mimeType = "text/event-stream",
                    )
                ]
            )
        ]
    )
    override fun accept(sseClient: SseClient) {
        val ctx = sseClient.ctx()
        LOGGER.info { "Processing request for getting message groups: ${ctx.queryString()}" }
        val request = checkNotNull(ctx.attribute<MessagesGroupRequest>(REQUEST_KEY)) {
            "request was not created in before handler"
        }


        val queue = ArrayBlockingQueue<Supplier<SseEvent>>(configuration.responseQueueSize)
        val handler = HttpMessagesRequestHandler(queue, sseResponseBuilder, dataMeasurement, maxMessagesPerRequest = configuration.bufferPerQuery,
            responseFormats = request.responseFormats ?: configuration.responseFormats)
        sseClient.onClose(handler::cancel)
        keepAliveHandler.addKeepAliveData(handler).use {
            searchMessagesHandler.loadMessageGroups(request, handler, dataMeasurement)
            sseClient.waitAndWrite(queue)
            LOGGER.info { "Processing search sse messages group request finished" }
        }
    }

    private fun createRequest(ctx: Context) = MessagesGroupRequest(
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
        rawOnly = ctx.queryParamAsClass<Boolean>(RAW_ONLY_PARAMETER)
            .getOrDefault(false),
        keepOpen = ctx.queryParamAsClass<Boolean>(KEEP_OPEN_PARAMETER)
            .getOrDefault(false),
        bookId = ctx.queryParamAsClass<BookId>(BOOK_ID_PARAM).get(),
        responseFormats = ctx.queryParams(RESPONSE_FORMAT).takeIf(List<*>::isNotEmpty)
            ?.mapTo(hashSetOf(), ResponseFormat.Companion::fromString),
    )

    companion object {
        const val ROUTE = "/search/sse/messages/group"
        private const val REQUEST_KEY = "sse.groups.request"
        private const val GROUP_PARAM = "group"
        private const val START_TIMESTAMP_PARAM = "startTimestamp"
        private const val END_TIMESTAMP_PARAM = "endTimestamp"
        private const val SORT_PARAMETER = "sort"
        private const val RAW_ONLY_PARAMETER = "onlyRaw"
        private const val KEEP_OPEN_PARAMETER = "keepOpen"
        private const val BOOK_ID_PARAM = "bookId"
        private const val RESPONSE_FORMAT = "responseFormat"
        private val LOGGER = KotlinLogging.logger { }
    }
}