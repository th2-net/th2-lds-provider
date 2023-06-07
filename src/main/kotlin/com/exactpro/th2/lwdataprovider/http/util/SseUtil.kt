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

package com.exactpro.th2.lwdataprovider.http.util

import io.javalin.http.Context
import io.javalin.http.Header
import io.javalin.http.NotFoundResponse
import io.javalin.http.sse.SseHandler
import mu.KLogger

const val EVENT_STREAM_CONTENT_TYPE = "text/event-stream"

val Context.isEventStream: Boolean
    get() = header(Header.ACCEPT) == EVENT_STREAM_CONTENT_TYPE

fun <T> Context.handleSseSequence(data: Sequence<T>, type: String, logger: KLogger, chunkSize: Int = 50) {
    require(chunkSize > 0) { "negative chunk size: $chunkSize" }
    if (!isEventStream) {
        throw NotFoundResponse("consider using $EVENT_STREAM_CONTENT_TYPE in ${Header.ACCEPT} header")
    }
    SseHandler(clientConsumer = {
        it.use { client ->
            data.chunked(chunkSize).forEachIndexed { index, chunk ->
                if (client.terminated()) {
                    logger.info { "Request is terminated. Stop processing data" }
                    return@forEachIndexed
                }
                client.sendEvent(
                    id = (index + 1).toString(),
                    event = type,
                    data = chunk,
                )
            }
        }
    }).handle(this)
}