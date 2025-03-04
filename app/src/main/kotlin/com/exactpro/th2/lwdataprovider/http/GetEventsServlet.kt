/*
 * Copyright 2021-2024 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.lwdataprovider.http.JavalinHandler.Companion.customSse
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

class GetEventsServlet(
    private val configuration: Configuration,
    private val sseResponseBuilder: SseResponseBuilder,
    private val keepAliveHandler: KeepAliveHandler,
    private val searchEventsHandler: SearchEventsHandler,
    private val convExecutor: Executor,
    private val dataMeasurement: DataMeasurement,
) : AbstractSseRequestHandler() {

    companion object {
        const val ROUTE = "/search/sse/events"
        private const val REQUEST_KEY = "sse.events.request"
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
        description = "returns stream of events that matches the specified parameters",
        queryParams = [
            OpenApiParam("startTimestamp", type = Long::class, required = true,
                description = "start timestamp for search", example = HttpServer.TIME_EXAMPLE),
            OpenApiParam("endTimestamp", type = Long::class,
                description = "end timestamp for search", example = HttpServer.TIME_EXAMPLE),
            OpenApiParam("parentEvent", type = String::class,
                description = "parent event id for search", example = "testEventId123"),
            OpenApiParam("searchDirection", type = SearchDirection::class,
                description = "defines the order of the events", example = "next"),
            OpenApiParam("resultCountLimit", type = Int::class,
                description = "limit for events in the response", example = "42"),
            OpenApiParam("bookId", type = String::class, required = true,
                description = "book ID for events", example = "book123"),
            OpenApiParam("scope", type = String::class, required = true,
                description = "scope for events", example = "scope123"),
            OpenApiParam("filters", type = Array<String>::class,
                description = "list of filters. Available filters are: type, name", example = "type"),
            // for type filter
            OpenApiParam("type-value", type = Array<String>::class,
                description = "values for type filter", example = "test"),
            OpenApiParam("type-negative", type = Boolean::class,
                description = "inverts the filter"),
            OpenApiParam("type-conjunct", type = Boolean::class,
                description = "actual value must match all filter values values"),
            // for type filter
            OpenApiParam("name-value", type = Array<String>::class,
                description = "values for name filter", example = "test"),
            OpenApiParam("name-negative", type = Boolean::class,
                description = "inverts the filter"),
            OpenApiParam("name-conjunct", type = Boolean::class,
                description = "actual value must match all filter values values"),
        ],
        responses = [
            OpenApiResponse(
                status = "200",
                content = [
                    OpenApiContent(from = Event::class, mimeType = "text/event-stream"),
                ],
                description = "event entity",
            )
        ]
    )
    override fun accept(sseClient: SseClient) {
        val ctx = sseClient.ctx()
        logger.info { "Received search sse event request with parameters: ${ctx.queryParamMap()}" }
        val request = checkNotNull(ctx.attribute<SseEventSearchRequest>(REQUEST_KEY)) {
            "request was not created in before handler"
        }

        val queue = ArrayBlockingQueue<Supplier<SseEvent>>(configuration.responseQueueSize)
        val reqContext = HttpGenericResponseHandler(
            queue,
            sseResponseBuilder,
            convExecutor,
            dataMeasurement,
            Event::eventId,
            SseResponseBuilder::build
        )
        sseClient.onClose(reqContext::cancel)
        keepAliveHandler.addKeepAliveData(reqContext).use {
            searchEventsHandler.loadEvents(request, reqContext)

            sseClient.waitAndWrite(queue)
            logger.info { "Processing search sse events request finished" }
        }
    }

    private fun createRequest(ctx: Context) = SseEventSearchRequest(
        startTimestamp = ctx.queryParamAsClass<Instant>("startTimestamp").get(),
        endTimestamp = ctx.queryParamAsClass<Instant>("endTimestamp")
            .allowNullable().get(),
        parentEvent = ctx.queryParamAsClass<ProviderEventId>("parentEvent")
            .allowNullable().get(),
        searchDirection = ctx.queryParamAsClass<SearchDirection>("searchDirection")
            .getOrDefault(SearchDirection.next),
        resultCountLimit = ctx.queryParamAsClass<Int>("resultCountLimit")
            .allowNullable()
            .check({
                it == null || it > 0
            }, "must be create than zero")
            .get(),
        bookId = ctx.queryParamAsClass<BookId>("bookId").get(),
        scope = ctx.queryParamAsClass<String>("scope").get(),
        filter = EventsFilterFactory.create(HttpFilterConverter.convert(ctx.queryParamMap())),
    )


}
