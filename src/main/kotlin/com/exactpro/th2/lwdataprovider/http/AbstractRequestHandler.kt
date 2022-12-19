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

import com.exactpro.th2.lwdataprovider.EventType
import com.exactpro.th2.lwdataprovider.SseEvent
import io.javalin.http.ContentType
import io.javalin.http.Context
import io.javalin.http.Handler
import io.javalin.http.Header
import io.javalin.http.HttpStatus
import java.util.concurrent.BlockingQueue
import java.util.concurrent.TimeUnit

abstract class AbstractRequestHandler : Handler, JavalinHandler {
    protected fun Context.waitAndWrite(
        queue: BlockingQueue<SseEvent>,
        timeout: Long,
        unit: TimeUnit,
        failureResult: (message: String) -> String,
    ) {
        header(Header.CACHE_CONTROL, "no-cache, no-store")
        contentType(ContentType.APPLICATION_JSON)

        try {
            val event = queue.poll(timeout, unit)
            if (event == null) {
                status(HttpStatus.REQUEST_TIMEOUT)
                    .result(failureResult("Operation hasn't done during timeout $timeout $unit"))
            } else {
                status(statusFromEventType(event.event))
                    .result(event.data)
            }
        } catch (e: RuntimeException) {
            status(HttpStatus.INTERNAL_SERVER_ERROR)
                .result(failureResult("${e.javaClass.simpleName}: ${e.message}"))
            throw e
        }
    }

    private fun statusFromEventType(event: EventType): HttpStatus {
        return if (event == EventType.ERROR) {
            HttpStatus.NOT_FOUND
        } else {
            HttpStatus.OK
        }
    }
}