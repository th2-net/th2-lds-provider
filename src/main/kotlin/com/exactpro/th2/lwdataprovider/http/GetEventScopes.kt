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
import com.exactpro.th2.lwdataprovider.entities.requests.util.invalidRequest
import com.exactpro.th2.lwdataprovider.handlers.SearchEventsHandler
import com.exactpro.th2.lwdataprovider.http.util.EVENT_STREAM_CONTENT_TYPE
import com.exactpro.th2.lwdataprovider.http.util.handleSseSequence
import io.javalin.Javalin
import io.javalin.http.Context
import io.javalin.http.HttpStatus
import io.javalin.http.pathParamAsClass
import io.javalin.http.queryParamAsClass
import io.javalin.openapi.ContentType
import io.javalin.openapi.HttpMethod
import io.javalin.openapi.OpenApi
import io.javalin.openapi.OpenApiContent
import io.javalin.openapi.OpenApiParam
import io.javalin.openapi.OpenApiResponse
import mu.KotlinLogging
import java.time.Instant

class GetEventScopes(
    private val handler: SearchEventsHandler,
) : JavalinHandler {

    @OpenApi(
        path = JSON_ROUTE,
        methods = [HttpMethod.GET],
        deprecated = true,
        description = "DEPRECATED: please use $SSE_ROUTE instead. " +
                "returns list of scopes for specified ${BOOK_ID_PARAM}. " +
                "If not $START_TIMESTAMP and $END_TIMESTAMP set returns all scopes. " +
                "Otherwise, returns scopes in the specified time interval.",
        pathParams = [
            OpenApiParam(
                name = BOOK_ID_PARAM,
                required = true,
                description = "book ID to request scopes",
                example = "bookId123",
            )
        ],
        queryParams = [
            OpenApiParam(
                name = START_TIMESTAMP,
                type = Long::class,
                description = "start timestamp to search for scopes: ${HttpServer.TIME_EXAMPLE}",
                required = false,
            ),
            OpenApiParam(
                name = END_TIMESTAMP,
                type = Long::class,
                description = "end timestamp to search for scopes: ${HttpServer.TIME_EXAMPLE}",
                required = false,
            ),
        ],
        responses = [
            OpenApiResponse(
                status = "200",
                content = [
                    OpenApiContent(from = Array<String>::class, mimeType = ContentType.JSON)
                ],
                description = """set of scopes for specified ${BOOK_ID_PARAM}. E.g. ["scope1","scope2"]"""
            ),
        ],
    )
    private fun handleJson(ctx: Context) {
        handleRequest(ctx) { scopes ->
            ctx.status(HttpStatus.OK).json(scopes.toSet())
        }
    }


    @OpenApi(
        path = SSE_ROUTE,
        methods = [HttpMethod.GET],
        description = "returns list of scopes for specified ${BOOK_ID_PARAM}. " +
                "If not $START_TIMESTAMP and $END_TIMESTAMP set returns all scopes. " +
                "Otherwise, returns scopes in the specified time interval",
        pathParams = [
            OpenApiParam(
                name = BOOK_ID_PARAM,
                required = true,
                description = "book ID to request scopes",
                example = "bookId123",
            )
        ],
        queryParams = [
            OpenApiParam(
                name = START_TIMESTAMP,
                type = Long::class,
                description = "start timestamp to search for scopes: ${HttpServer.TIME_EXAMPLE}",
                required = false,
            ),
            OpenApiParam(
                name = END_TIMESTAMP,
                type = Long::class,
                description = "end timestamp to search for scopes: ${HttpServer.TIME_EXAMPLE}",
                required = false,
            ),
            OpenApiParam(
                name = CHUNK_SIZE,
                type = Int::class,
                description = "the chunk size for response in SSE format. Default value is 50",
                required = false
            ),
        ],
        responses = [
            OpenApiResponse(
                status = "200",
                content = [
                    OpenApiContent(from = Array<String>::class, mimeType = EVENT_STREAM_CONTENT_TYPE)
                ],
                description = """set of scopes for specified ${BOOK_ID_PARAM}. E.g. ["scope1","scope2"] in SSE format"""
            ),
        ],
    )
    private fun handleSse(ctx: Context) {
        handleRequest(ctx) { scopes ->
            val chunkSize = queryParamAsClass<Int>(CHUNK_SIZE).check({ it > 0 }, "NEGATIVE_CHUNK_SIZE").getOrDefault(50)
            handleSseSequence(scopes, "scope", chunkSize = chunkSize)
        }
    }

    private inline fun handleRequest(ctx: Context, handlerFunction: Context.(Sequence<String>) -> Unit) {
        val bookId = ctx.pathParamAsClass<BookId>(BOOK_ID_PARAM).get()
        val startTimestamp = ctx.queryParamAsClass<Instant>(START_TIMESTAMP).allowNullable()
            .get()
        val endTimestamp = ctx.queryParamAsClass<Instant>(END_TIMESTAMP).allowNullable()
            .get()
        LOGGER.info { "Loading scopes for book $bookId: (from: $startTimestamp, to: $endTimestamp)" }
        val scopes: Sequence<String> = runCatching {
            when {
                startTimestamp == null && endTimestamp == null ->
                    handler.loadAllScopes(bookId).asSequence() // FIXME: should be an iterator
                startTimestamp != null && endTimestamp != null ->
                    handler.loadScopes(bookId, startTimestamp, endTimestamp).asSequence()

                else -> {
                    invalidRequest("either both $START_TIMESTAMP and $END_TIMESTAMP must be specified or neither of them")
                }
            }
        }.onFailure {
            LOGGER.error(it) { "cannot process scopes request" }
        }.getOrThrow()

        ctx.handlerFunction(scopes)
    }

    override fun setup(app: Javalin) {
        app.get(JSON_ROUTE, this::handleJson)
        app.get(SSE_ROUTE, this::handleSse)
    }

    companion object {
        private const val BOOK_ID_PARAM = "bookId"
        private const val START_TIMESTAMP = "startTimestamp"
        private const val END_TIMESTAMP = "endTimestamp"
        private const val CHUNK_SIZE = "chunkedSize"
        private val LOGGER = KotlinLogging.logger { }

        const val JSON_ROUTE = "/book/{$BOOK_ID_PARAM}/event/scopes"
        const val SSE_ROUTE = "/book/{$BOOK_ID_PARAM}/event/scopes/sse"
    }
}