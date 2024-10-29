/*
 * Copyright 2024 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.lwdataprovider.entities.internal.ProviderEventId
import com.exactpro.th2.lwdataprovider.entities.requests.SearchDirection
import com.exactpro.th2.lwdataprovider.entities.requests.SseEventSearchRequest
import com.exactpro.th2.lwdataprovider.entities.requests.converter.HttpFilterConverter
import com.exactpro.th2.lwdataprovider.entities.responses.Event
import com.exactpro.th2.lwdataprovider.filter.events.EventsFilterFactory
import com.exactpro.th2.lwdataprovider.handlers.SearchEventsHandler
import com.exactpro.th2.lwdataprovider.http.util.JSON_STREAM_CONTENT_TYPE
import com.exactpro.th2.lwdataprovider.http.util.writeJsonStream
import com.exactpro.th2.lwdataprovider.workers.KeepAliveHandler
import io.github.oshai.kotlinlogging.KotlinLogging
import io.javalin.Javalin
import io.javalin.http.Context
import io.javalin.http.queryParamAsClass
import io.javalin.openapi.HttpMethod
import io.javalin.openapi.OpenApi
import io.javalin.openapi.OpenApiContent
import io.javalin.openapi.OpenApiParam
import io.javalin.openapi.OpenApiResponse
import java.time.Instant
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.Executor
import java.util.function.Supplier

class DownloadEventsHandler(
    private val configuration: Configuration,
    private val convExecutor: Executor,
    private val sseResponseBuilder: SseResponseBuilder,
    private val keepAliveHandler: KeepAliveHandler,
    private val searchEventsHandler: SearchEventsHandler,
    private val dataMeasurement: DataMeasurement,
) : JavalinHandler {
    override fun setup(app: Javalin, context: JavalinContext) {
        app.get(ROUTE_DOWNLOAD_EVENTS, this::handleMessage)
    }

    @OpenApi(
        path = ROUTE_DOWNLOAD_EVENTS,
        description = "returns list of messages for specified groups. Each group will be requested one after another " +
                "(there is no order guaranties between groups). Messages for a group are not sorted by default. ",
        queryParams = [
            OpenApiParam(START_TIMESTAMP_PARAM, type = Long::class, required = true, example = HttpServer.TIME_EXAMPLE,
                description = "start timestamp for search. Epoch time in milliseconds"),
            OpenApiParam(END_TIMESTAMP_PARAM, type = Long::class, required = true, example = HttpServer.TIME_EXAMPLE,
                description = "end timestamp for search. Epoch time in milliseconds"),
            OpenApiParam(PARENT_EVENT_PARAM, type = String::class,
                description = "parent event id for search", example = "testEventId123"),
            OpenApiParam(KEEP_OPEN_PARAMETER, type = Boolean::class,
                description = "enables pulling for updates until have not found a message outside the requested interval"),
            OpenApiParam(BOOK_ID_PARAM, required = true, example = "bookId123",
                description = "book ID for requested groups"),
            OpenApiParam(SCOPE_PARAM, type = String::class, required = true,
                description = "scope for events", example = "scope123"),
            OpenApiParam(LIMIT, type = Int::class,
                description = "limit for events in the response. No limit if not specified"),
            OpenApiParam(SEARCH_DIRECTION, type = SearchDirection::class, example = "next",
                description = "defines the order of the events"),

            OpenApiParam("filters", type = Array<String>::class, isRepeatable = true,
                description = "list of filters. Available filters are: type, name", example = "type"),
            // for type filter
            OpenApiParam("type-value", type = Array<String>::class, isRepeatable = true,
                description = "values for type filter", example = "test"),
            OpenApiParam("type-negative", type = Boolean::class,
                description = "inverts the filter"),
            OpenApiParam("type-conjunct", type = Boolean::class,
                description = "actual value must match all filter values values"),
            // for type filter
            OpenApiParam("name-value", type = Array<String>::class, isRepeatable = true,
                description = "values for name filter", example = "test"),
            OpenApiParam("name-negative", type = Boolean::class,
                description = "inverts the filter"),
            OpenApiParam("name-conjunct", type = Boolean::class,
                description = "actual value must match all filter values values"),
        ],
        methods = [HttpMethod.GET],
        responses = [
            OpenApiResponse(status = "200", content = [
                OpenApiContent(from = Event::class, mimeType = JSON_STREAM_CONTENT_TYPE)
            ])
        ]
    )

    private fun createRequest(ctx: Context) = SseEventSearchRequest(
        startTimestamp = ctx.queryParamAsClass<Instant>(START_TIMESTAMP_PARAM).get(),
        endTimestamp = ctx.queryParamAsClass<Instant>(END_TIMESTAMP_PARAM)
            .allowNullable().get(),
        parentEvent = ctx.queryParamAsClass<ProviderEventId>(PARENT_EVENT_PARAM)
            .allowNullable().get(),
        searchDirection = ctx.queryParamAsClass<SearchDirection>(SEARCH_DIRECTION)
            .getOrDefault(SearchDirection.next),
        resultCountLimit = ctx.queryParamAsClass<Int>(LIMIT)
            .allowNullable()
            .check({
                it == null || it > 0
            }, "must be create than zero")
            .get(),
        bookId = ctx.queryParamAsClass<BookId>(BOOK_ID_PARAM).get(),
        scope = ctx.queryParamAsClass<String>(SCOPE_PARAM).get(),
        filter = EventsFilterFactory.create(HttpFilterConverter.convert(ctx.queryParamMap())),
    )

    private fun handleMessage(ctx: Context) {
        val request = createRequest(ctx)

        val queue = ArrayBlockingQueue<Supplier<SseEvent>>(configuration.responseQueueSize)
        val handler = HttpGenericResponseHandler(
            queue, sseResponseBuilder, convExecutor, dataMeasurement,
            Event::eventId,
            SseResponseBuilder::build
        )
        keepAliveHandler.addKeepAliveData(handler).use {
            searchEventsHandler.loadEvents(request, handler)
            writeJsonStream(ctx, queue, handler, dataMeasurement, LOGGER)
            LOGGER.info { "Processing search sse messages group request finished" }
        }
    }

    companion object {
        private const val START_TIMESTAMP_PARAM = "startTimestamp"
        private const val END_TIMESTAMP_PARAM = "endTimestamp"
        private const val PARENT_EVENT_PARAM = "parentEvent"
        private const val KEEP_OPEN_PARAMETER = "keepOpen"
        private const val BOOK_ID_PARAM = "bookId"
        private const val SCOPE_PARAM = "scope"
        private const val LIMIT = "limit"
        private const val SEARCH_DIRECTION = "searchDirection"
        private val LOGGER = KotlinLogging.logger { }
        const val ROUTE_DOWNLOAD_EVENTS = "/download/events"
    }
}