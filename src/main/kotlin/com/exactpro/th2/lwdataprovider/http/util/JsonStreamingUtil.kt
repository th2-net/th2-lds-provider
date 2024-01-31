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

import com.exactpro.th2.lwdataprovider.EventType
import com.exactpro.th2.lwdataprovider.SseEvent
import com.exactpro.th2.lwdataprovider.db.DataMeasurement
import com.exactpro.th2.lwdataprovider.http.HttpMessagesRequestHandler
import com.exactpro.th2.lwdataprovider.http.listener.DEFAULT_PROCESS_LISTENER
import com.exactpro.th2.lwdataprovider.http.listener.ProgressListener
import com.exactpro.th2.lwdataprovider.metrics.HttpWriteMetrics
import com.exactpro.th2.lwdataprovider.metrics.ResponseQueue
import io.javalin.http.Context
import io.javalin.http.Header
import io.javalin.http.HttpStatus
import mu.KLogger
import org.apache.commons.lang3.StringUtils
import java.util.concurrent.ArrayBlockingQueue
import java.util.function.Supplier

const val JSON_STREAM_CONTENT_TYPE = "application/stream+json"

fun writeJsonStream(
    ctx: Context,
    queue: ArrayBlockingQueue<Supplier<SseEvent>>,
    handler: HttpMessagesRequestHandler,
    dataMeasurement: DataMeasurement,
    logger: KLogger,
    progressListener: ProgressListener = DEFAULT_PROCESS_LISTENER,
) {
    progressListener.onStart()

    val matchedPath = ctx.matchedPath()
    var dataSent = 0

    var writeHeader = true
    var status: HttpStatus = HttpStatus.OK

    fun writeHeader() {
        if (writeHeader) {
            ctx.status(status)
                .contentType(JSON_STREAM_CONTENT_TYPE)
                .header(Header.TRANSFER_ENCODING, "chunked")
            writeHeader = false
        }
    }

    val output = ctx.res().outputStream.buffered()
    try {
        do {
            dataMeasurement.start("process_sse_event").use {
                val nextEvent = queue.take()
                ResponseQueue.currentSize(matchedPath, queue.size)
                val sseEvent = dataMeasurement.start("await_convert_to_json").use { nextEvent.get() }
                if (writeHeader && sseEvent is SseEvent.ErrorData.SimpleError) {
                    // something happened during request
                    status = HttpStatus.INTERNAL_SERVER_ERROR
                }
                writeHeader()
                if (sseEvent is SseEvent.ErrorData) {
                    progressListener.onError(sseEvent)
                }
                when (sseEvent.event) {
                    EventType.KEEP_ALIVE -> output.flush()
                    EventType.CLOSE -> {
                        logger.info { "Received close event" }
                        return
                    }

                    else -> {
                        logger.debug {
                            "Write event to output: ${
                                StringUtils.abbreviate(sseEvent.data.toString(SseEvent.DATA_CHARSET), 100)
                            }"
                        }
                        output.write(sseEvent.data)
                        output.write('\n'.code)
                        dataSent++
                    }
                }
                if (queue.isEmpty() && !handler.isAlive) {
                    logger.info { "Request canceled" }
                    return
                }
            }
        } while (true)
    } catch (ex: Exception) {
        logger.error(ex) { "cannot process next event" }
        progressListener.onError(ex)
        handler.cancel()
        queue.clear()
    } finally {
        if (handler.isAlive) {
            progressListener.onCompleted()
        } else {
            progressListener.onCanceled()
        }
        HttpWriteMetrics.messageSent(matchedPath, dataSent)
        runCatching { output.flush() }
            .onFailure { logger.error(it) { "cannot flush the remaining data when processing is finished" } }
    }
}