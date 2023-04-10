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
import com.exactpro.th2.lwdataprovider.metrics.HttpWriteMetrics
import com.exactpro.th2.lwdataprovider.metrics.ResponseQueue
import io.javalin.http.Context
import io.javalin.validation.Validator
import mu.KotlinLogging
import org.apache.commons.lang3.StringUtils
import java.util.concurrent.BlockingQueue
import java.util.function.Consumer
import java.util.function.Supplier

abstract class AbstractSseRequestHandler : Consumer<SseClient>, JavalinHandler {
    
    protected fun SseClient.waitAndWrite(
        queue: BlockingQueue<Supplier<SseEvent>>,
    ) {

        val matchedPath = ctx().matchedPath()
        var inProcess = true
        var dataSent = 0
        try {
            while (inProcess) {
                val supplier = queue.take()
                ResponseQueue.currentSize(matchedPath, queue.size)
                // com.exactpro.th2.lwdataprovider.http.HttpMessagesRequestHandler$$Lambda$812.0x00000008007db440.get ()	156,703 ms (58.9%)	2,673 ms (2.7%)
                val event = supplier.get()
                if (terminated()) {
                    K_LOGGER.info { "Request is terminated. Clear queue and stop processing" }
                    queue.clear()
                    return
                }
                HttpWriteMetrics.measureWrite(matchedPath) {
                    sendEvent(
                        event.event.typeName,
                        event.data,
                        event.metadata,
                    )
                }
                if (event.event == EventType.EVENT || event.event == EventType.MESSAGE) {
                    dataSent++
                }
                if (event.event == EventType.KEEP_ALIVE || event.event == EventType.ERROR) {
                    // flush on keep alive to avoid closing connection because of inactivity
                    // flush after error to deliver it to the user
                    flush()
                }
                K_LOGGER.debug { "Sent sse event: type ${event.event}, metadata ${event.metadata}, data ${StringUtils.abbreviate(event.data, 50)}" }
                if (event.event == EventType.CLOSE) {
                    close()
                    inProcess = false
                    val size = queue.size
                    if (size > 0) {
                        K_LOGGER.warn { "There are $size event(s) left in queue" }
                    }
                }
            }
        } finally {
            HttpWriteMetrics.messageSent(matchedPath, dataSent)
        }
    }

    protected fun Context.listQueryParameters(name: String): Validator<List<String>> =
        Validator(fieldName = name, typedValue = queryParams(name))

    companion object {
        private val K_LOGGER = KotlinLogging.logger { }
    }
}