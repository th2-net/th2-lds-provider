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
import com.exactpro.th2.lwdataprovider.ExceptionInfo
import com.exactpro.th2.lwdataprovider.entities.exceptions.InvalidRequestException
import com.exactpro.th2.lwdataprovider.entities.requests.util.invalidRequest
import com.exactpro.th2.lwdataprovider.handlers.SearchMessagesHandler
import io.javalin.Javalin
import io.javalin.http.Context
import io.javalin.http.Handler
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
import org.apache.commons.lang3.exception.ExceptionUtils
import java.time.Instant

class GetSessionAliases(
    private val handler: SearchMessagesHandler,
) : Handler, JavalinHandler {

    @OpenApi(
        path = ROUTE,
        methods = [HttpMethod.GET],
        description = "returns list of session aliases for specified ${BOOK_ID_PARAM}. " +
                "If not $START_TIMESTAMP and $END_TIMESTAMP set returns all aliases. " +
                "Otherwise, returns aliases in the specified time interval",
        pathParams = [
            OpenApiParam(
                name = BOOK_ID_PARAM,
                description = "book ID",
                required = true,
                example = "bookId123",
            )
        ],
        queryParams = [
            OpenApiParam(
                name = START_TIMESTAMP,
                type = Long::class,
                description = "start timestamp to search for aliases: ${HttpServer.TIME_EXAMPLE}",
                required = false,
            ),
            OpenApiParam(
                name = END_TIMESTAMP,
                type = Long::class,
                description = "end timestamp to search for aliases: ${HttpServer.TIME_EXAMPLE}",
                required = false,
            ),
        ],
        responses = [
            OpenApiResponse(
                status = "200",
                content = [
                    OpenApiContent(from = Array<String>::class, mimeType = ContentType.JSON)
                ],
                description = "list of aliases for specified book",
            ),
        ],
    )
    override fun handle(ctx: Context) {
        ctx.contentType(io.javalin.http.ContentType.APPLICATION_JSON)
        val bookId = ctx.pathParamAsClass<BookId>(BOOK_ID_PARAM).get()
        val startTimestamp = ctx.queryParamAsClass<Instant>(START_TIMESTAMP).allowNullable()
            .get()
        val endTimestamp = ctx.queryParamAsClass<Instant>(END_TIMESTAMP).allowNullable()
            .get()
        LOGGER.info { "Loading aliases for book $bookId (from: $startTimestamp, to: $endTimestamp)" }
        try {
            val aliases: Collection<String> = when {
                startTimestamp == null && endTimestamp == null ->
                    handler.extractAllStreamNames(bookId)
                startTimestamp != null && endTimestamp != null ->
                    handler.extractStreamNames(bookId, startTimestamp, endTimestamp).asSequence().toSet()
                else ->
                    invalidRequest("either both $START_TIMESTAMP and $END_TIMESTAMP must be specified or neither of them")
            }
            // TODO: should be streaming API
            ctx.status(HttpStatus.OK).json(aliases)
        } catch (ex: Exception) {
            LOGGER.error(ex) { "cannot process aliases request" }
            if (ex is InvalidRequestException) {
                throw ex
            }
            ctx.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .json(ExceptionInfo(ex::class.java.canonicalName, ExceptionUtils.getRootCauseMessage(ex)))
        }
    }

    override fun setup(app: Javalin) {
        app.get(ROUTE, this)
    }

    companion object {
        private const val BOOK_ID_PARAM = "bookId"
        private const val START_TIMESTAMP = "startTimestamp"
        private const val END_TIMESTAMP = "endTimestamp"
        private val LOGGER = KotlinLogging.logger { }

        const val ROUTE = "/book/{$BOOK_ID_PARAM}/message/aliases"
    }
}