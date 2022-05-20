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

import com.exactpro.th2.lwdataprovider.SseEvent
import com.exactpro.th2.lwdataprovider.SseResponseBuilder
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import com.exactpro.th2.lwdataprovider.db.DataMeasurement
import com.exactpro.th2.lwdataprovider.entities.requests.MessagesGroupRequest
import com.exactpro.th2.lwdataprovider.handlers.SearchMessagesHandler
import com.exactpro.th2.lwdataprovider.workers.KeepAliveHandler
import mu.KotlinLogging
import java.util.concurrent.ArrayBlockingQueue
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

class GetMessageGroupsServlet(
    private val configuration: Configuration,
    private val sseResponseBuilder: SseResponseBuilder,
    private val keepAliveHandler: KeepAliveHandler,
    private val searchMessagesHandler: SearchMessagesHandler,
    private val dataMeasurement: DataMeasurement,
) : SseServlet() {

    override fun doGet(req: HttpServletRequest, resp: HttpServletResponse) {
        LOGGER.info { "Processing request for getting message groups: ${req.queryString}" }
        val queryParametersMap = getParameters(req)
        val request = MessagesGroupRequest.fromParametersMap(queryParametersMap)


        val queue = ArrayBlockingQueue<SseEvent>(configuration.responseQueueSize)
        val handler = HttpMessagesRequestHandler(queue, sseResponseBuilder, dataMeasurement, maxMessagesPerRequest = configuration.bufferPerQuery)
//        reqContext.startStep("messages_loading").use {
            keepAliveHandler.addKeepAliveData(handler).use {
                searchMessagesHandler.loadMessageGroups(request, handler, dataMeasurement)
                this.waitAndWrite(queue, resp)
                LOGGER.info { "Processing search sse messages group request finished" }
            }

//        }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }
}