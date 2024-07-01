/*
 * Copyright 2022-2024 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.lwdataprovider.entities.requests.SsePageInfosSearchRequest
import com.exactpro.th2.lwdataprovider.entities.responses.PageInfo
import com.exactpro.th2.lwdataprovider.handlers.GeneralCradleHandler
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
import java.util.concurrent.Executor
import java.util.function.Supplier

class GetPageInfosServlet(
    private val configuration: Configuration,
    private val sseResponseBuilder: SseResponseBuilder,
    private val keepAliveHandler: KeepAliveHandler,
    private val handler: GeneralCradleHandler,
    private val convExecutor: Executor,
    private val dataMeasurement: DataMeasurement,
) : AbstractSseRequestHandler() {

    override fun setup(app: Javalin, context: JavalinContext) {
        app.before(ROUTE) {
            it.attribute(REQUEST_KEY, createRequest(it))
        }
        app.customSse(ROUTE, this, context)
    }

    @OpenApi(
        path = ROUTE,
        description = "returns the stream of the page infos stored in Cradle and related to book and time interval",
        methods = [HttpMethod.GET],
        queryParams = [
            OpenApiParam("bookId", type = String::class, required = true,
                description = "book ID for page infos", example = "book123"),
            OpenApiParam("startTimestamp", type = Long::class, required = true,
                description = "start timestamp for search", example = HttpServer.TIME_EXAMPLE),
            OpenApiParam("endTimestamp", type = Long::class, required = true,
                description = "end timestamp for search", example = HttpServer.TIME_EXAMPLE),
            OpenApiParam("resultCountLimit", type = Int::class,
                description = "limit for page infos in the response", example = "42"),
        ],
        responses = [
            OpenApiResponse(
                status = "200",
                content = [
                    OpenApiContent(from = PageInfo::class, mimeType = "text/event-stream")
                ],
                description = """page info in cradle. E.g 
                      {
                        "id": {"book":"book_name","name":"page_name"},
                        "comment": "comment_example",
                        "started": {"epochSecond":1668024000,"nano":0},
                        "ended": {"epochSecond":1668024000,"nano":0},
                        "updated": {"epochSecond":1668024000,"nano":0},
                        "removed": {"epochSecond":1668024000,"nano":0},
                      }"""
            )
        ]
    )
    override fun accept(sseClient: SseClient) {
        val ctx = sseClient.ctx()
        K_LOGGER.info { "Received search sse page info(s) request with parameters: ${ctx.queryParamMap()}" }
        val request = checkNotNull(ctx.attribute<SsePageInfosSearchRequest>(REQUEST_KEY)) {
            "request was not created in before handler"
        }

        val queue = ArrayBlockingQueue<Supplier<SseEvent>>(configuration.responseQueueSize)
        val reqContext = HttpGenericResponseHandler(
            queue,
            sseResponseBuilder,
            convExecutor,
            dataMeasurement,
            PageInfo::id,
            SseResponseBuilder::build,
        )
        sseClient.onClose(reqContext::cancel)
        keepAliveHandler.addKeepAliveData(reqContext).use {
            handler.getPageInfos(request, reqContext)

            sseClient.waitAndWrite(queue)
            K_LOGGER.info { "Processing search sse page infos request finished" }
        }
    }

    private fun createRequest(ctx: Context) = SsePageInfosSearchRequest(
        bookId = ctx.queryParamAsClass<BookId>("bookId").get(),
        startTimestamp = ctx.queryParamAsClass<Instant>("startTimestamp").get(),
        endTimestamp = ctx.queryParamAsClass<Instant>("endTimestamp").get(),
        resultCountLimit = ctx.queryParamAsClass<Int>("resultCountLimit")
            .allowNullable()
            .check({
                it == null || it > 0
            }, "must be create than zero")
            .get(),
    )

    companion object {
        const val ROUTE = "/search/sse/page-infos"
        private const val REQUEST_KEY = "sse.page-infos.request"

        private val K_LOGGER = KotlinLogging.logger { }
    }
}