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

import com.exactpro.th2.lwdataprovider.handlers.GeneralCradleHandler
import io.javalin.Javalin
import io.javalin.http.Context
import io.javalin.http.Handler
import io.javalin.http.HttpStatus
import io.javalin.openapi.HttpMethod
import io.javalin.openapi.OpenApi
import io.javalin.openapi.OpenApiContent
import io.javalin.openapi.OpenApiResponse
import mu.KotlinLogging

class GetBookIDs(
    private val handler: GeneralCradleHandler,
) : Handler, JavalinHandler {

    @OpenApi(
        path = ROUTE,
        description = "returns the list of the book IDs stored in Cradle ",
        methods = [HttpMethod.GET],
        responses = [
            OpenApiResponse(
                status = "200",
                content = [
                    OpenApiContent(from = Array<String>::class)
                ],
                description = "list of book IDs in cradle. E.g [\"book1\",\"book2\"]",
            )
        ]
    )
    override fun handle(ctx: Context) {
        LOGGER.info { "Loading books from cradle" }
        val books = handler.getBookIDs().map { it.name }
        LOGGER.info { "${books.size} book(s) loaded: $books" }

        ctx.status(HttpStatus.OK)
            .json(books)
    }

    override fun setup(app: Javalin) {
        app.get(ROUTE, this)
    }

    companion object {
        const val ROUTE = "/books"

        private val LOGGER = KotlinLogging.logger {}
    }
}