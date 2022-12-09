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
import com.exactpro.th2.lwdataprovider.db.CradleMessageExtractor
import com.exactpro.th2.lwdataprovider.handlers.SearchMessagesHandler
import io.javalin.Javalin
import io.javalin.http.Context
import io.javalin.http.Handler
import io.javalin.http.HttpStatus
import io.javalin.http.pathParamAsClass
import io.javalin.openapi.ContentType
import io.javalin.openapi.HttpMethod
import io.javalin.openapi.OpenApi
import io.javalin.openapi.OpenApiContent
import io.javalin.openapi.OpenApiParam
import io.javalin.openapi.OpenApiResponse
import mu.KotlinLogging
import org.apache.commons.lang3.exception.ExceptionUtils

class GetMessageGroups(
    private val handler: SearchMessagesHandler,
) : Handler, JavalinHandler {

    @OpenApi(
        path = ROUTE,
        methods = [HttpMethod.GET],
        description = "returns list of groups for specified $BOOK_ID_PARAM",
        pathParams = [
            OpenApiParam(
                name = BOOK_ID_PARAM,
                required = true,
                description = "book ID to request groups",
                example = "bookId123",
            )
        ],
        responses = [
            OpenApiResponse(
                status = "200",
                content = [
                    OpenApiContent(from = Array<String>::class, mimeType = ContentType.JSON)
                ],
                description = """set of groups for specified $BOOK_ID_PARAM. E.g. ["group1","group2"]"""
            )
        ]
    )
    override fun handle(ctx: Context) {
        val bookId = ctx.pathParamAsClass<BookId>(BOOK_ID_PARAM).get()
        LOGGER.info { "Loading groups for book $bookId" }
        try {
            val groups: Set<String> = handler.extractGroups(bookId)
            ctx.status(HttpStatus.OK).json(groups)
        } catch (ex: Exception) {
            ctx.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .json(ExceptionInfo(ex::class.java.canonicalName, ExceptionUtils.getRootCauseMessage(ex)))
        }
    }

    override fun setup(app: Javalin) {
        app.get(ROUTE, this)
    }

    companion object {
        private const val BOOK_ID_PARAM = "bookId"
        private val LOGGER = KotlinLogging.logger { }

        const val ROUTE = "/book/{$BOOK_ID_PARAM}/message/groups"
    }
}