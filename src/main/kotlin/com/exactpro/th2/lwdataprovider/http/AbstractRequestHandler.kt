/*******************************************************************************
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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
 ******************************************************************************/

package com.exactpro.th2.lwdataprovider.http

import com.exactpro.th2.lwdataprovider.SseEvent
import io.javalin.http.ContentType
import io.javalin.http.Context
import io.javalin.http.Handler
import io.javalin.http.Header
import io.javalin.http.HttpStatus
import java.util.concurrent.BlockingQueue
import java.util.function.Supplier

abstract class AbstractRequestHandler : Handler, JavalinHandler {
    protected fun Context.waitAndWrite(
        queue: BlockingQueue<Supplier<SseEvent>>,
        failureResult: (message: String) -> String,
    ) {
        try {
            val supplier = checkNotNull(queue.take()) { "queue returned null event" }
            val event = supplier.get()
            status(statusFromEventType(event))
                .defaultHeaders()
                .result(event.data)

        } catch (e: Exception) {
            status(HttpStatus.INTERNAL_SERVER_ERROR)
                .defaultHeaders()
                .result(failureResult("${e.javaClass.simpleName}: ${e.message}"))
            throw e
        }
    }

    private fun statusFromEventType(event: SseEvent): HttpStatus {
        return when (event) {
            is SseEvent.ErrorData.SimpleError -> HttpStatus.NOT_FOUND
            is SseEvent.ErrorData.TimeoutError -> HttpStatus.REQUEST_TIMEOUT
            is SseEvent.ErrorData -> HttpStatus.INTERNAL_SERVER_ERROR
            else -> HttpStatus.OK
        }
    }

    private fun Context.defaultHeaders(): Context = apply {
        header(Header.CACHE_CONTROL, "no-cache, no-store")
        contentType(ContentType.APPLICATION_JSON)
    }
}