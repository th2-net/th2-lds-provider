/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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

import io.javalin.http.Context
import io.javalin.http.Handler
import io.javalin.http.Header
import io.javalin.http.HttpStatus
import java.util.function.Consumer
import java.util.function.Function

/**
 * Copied from [io.javalin.http.sse.SseHandler]
 * because we need custom client implementation
 */
class SseHandler @JvmOverloads constructor(
    private val timeout: Long = 0,
    private val clientConsumer: Consumer<SseClient>,
    private val clientSupplier: Function<Context, SseClient>,
) : Handler {

    override fun handle(ctx: Context) {
        try {
            if (ctx.header(Header.ACCEPT) == EVENT_STREAM_CONTENT_TYPE) {
                ctx.res().apply {
                    status = 200
                    characterEncoding = "UTF-8"
                    contentType = EVENT_STREAM_CONTENT_TYPE
                    addHeader(Header.CONNECTION, "close")
                    addHeader(Header.CACHE_CONTROL, "no-cache")
                    addHeader(Header.X_ACCEL_BUFFERING, "no") // See https://serverfault.com/a/801629
                    flushBuffer()
                }
                clientConsumer.accept(clientSupplier.apply(ctx))
            } else {
                ctx.status(HttpStatus.NOT_FOUND)
                    .result("consider using $EVENT_STREAM_CONTENT_TYPE in ${Header.ACCEPT} header")
            }
        } catch (e: Exception) { // TODO: maybe move to our Servlet implementations
            ctx.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .result(e.message ?: "Request can't be handled by the ${e::class.java} error with ${e.message} message")
        }
    }

    companion object {
        const val EVENT_STREAM_CONTENT_TYPE: String = "text/event-stream"
    }
}