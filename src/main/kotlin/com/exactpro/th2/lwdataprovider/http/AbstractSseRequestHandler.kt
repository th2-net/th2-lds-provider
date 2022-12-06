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
import io.javalin.http.sse.SseClient
import java.util.concurrent.BlockingQueue
import java.util.function.Consumer

abstract class AbstractSseRequestHandler : Consumer<SseClient>, JavalinHandler {
    
    protected fun SseClient.waitAndWrite(
        queue: BlockingQueue<SseEvent>,
    ) {

        var inProcess = true
        while (inProcess) {
            val event = queue.take()
            if (event.event == EventType.CLOSE) {
                close()
                inProcess = false
            } else {
                sendEvent(
                    event.event.typeName,
                    event.data,
                    event.metadata,
                )
            }
        }
    }
}