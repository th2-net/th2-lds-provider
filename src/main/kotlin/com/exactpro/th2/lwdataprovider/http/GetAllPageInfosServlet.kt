/*
 * Copyright 2022 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.lwdataprovider.entities.requests.AllPageInfoRequest
import com.exactpro.th2.lwdataprovider.entities.responses.PageInfo
import com.exactpro.th2.lwdataprovider.handlers.GeneralCradleHandler
import com.exactpro.th2.lwdataprovider.http.JavalinHandler.Companion.customSse
import com.exactpro.th2.lwdataprovider.workers.KeepAliveHandler
import io.javalin.Javalin
import io.javalin.http.Context
import io.javalin.http.pathParamAsClass
import io.javalin.http.queryParamAsClass
import io.javalin.openapi.HttpMethod
import io.javalin.openapi.OpenApi
import io.javalin.openapi.OpenApiContent
import io.javalin.openapi.OpenApiParam
import io.javalin.openapi.OpenApiResponse
import mu.KotlinLogging
import java.util.concurrent.ArrayBlockingQueue
import java.util.function.Supplier

class GetAllPageInfosServlet(
    private val configuration: Configuration,
    private val sseResponseBuilder: SseResponseBuilder,
    private val keepAliveHandler: KeepAliveHandler,
    private val handler: GeneralCradleHandler,
) : AbstractSseRequestHandler() {

    override fun setup(app: Javalin, context: JavalinContext) {
        app.before(ROUTE) {
            it.attribute(REQUEST_KEY, createRequest(it))
        }
        app.customSse(ROUTE, this, context)
    }

    @OpenApi(
        path = ROUTE,
        description = "returns the stream of all page infos stored in Cradle",
        methods = [HttpMethod.GET],
        pathParams = [
            OpenApiParam(
                "bookId", type = String::class, required = true,
                description = "book ID for page infos", example = "book123"
            ),
        ],
        queryParams = [
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
        K_LOGGER.info { "Received all page info request with parameters: ${ctx.queryParamMap()}" }
        val request = checkNotNull(ctx.attribute<AllPageInfoRequest>(REQUEST_KEY)) {
            "request was not created in before handler"
        }

        val queue = ArrayBlockingQueue<Supplier<SseEvent>>(configuration.responseQueueSize)
        val reqContext = HttpGenericResponseHandler(queue, sseResponseBuilder, PageInfo::id, SseResponseBuilder::build)
        sseClient.onClose(reqContext::cancel)
        keepAliveHandler.addKeepAliveData(reqContext).use {
            handler.getAllPageInfos(request, reqContext)

            sseClient.waitAndWrite(queue)
            K_LOGGER.info { "Processing all page infos request finished" }
        }
    }

    private fun createRequest(ctx: Context) = AllPageInfoRequest(
        bookId = ctx.pathParamAsClass<BookId>(BOOK_ID).get(),
        limit = ctx.queryParamAsClass<Int>(RESULT_LIMIT)
            .allowNullable()
            .check({
                it == null || it > 0
            }, "must be create than zero")
            .get(),
    )

    companion object {
        private const val BOOK_ID = "bookId"
        private const val RESULT_LIMIT = "resultCountLimit"
        const val ROUTE = "/search/sse/page-infos/{$BOOK_ID}/all"
        private const val REQUEST_KEY = "sse.page-infos.all.request"

        private val K_LOGGER = KotlinLogging.logger { }
    }
}