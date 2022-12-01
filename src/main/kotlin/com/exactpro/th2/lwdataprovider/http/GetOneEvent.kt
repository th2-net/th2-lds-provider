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
import com.exactpro.th2.lwdataprovider.SseResponseBuilder
import com.exactpro.th2.lwdataprovider.entities.internal.ProviderEventId
import com.exactpro.th2.lwdataprovider.entities.requests.GetEventRequest
import com.exactpro.th2.lwdataprovider.handlers.SearchEventsHandler
import com.exactpro.th2.lwdataprovider.workers.KeepAliveHandler
import mu.KotlinLogging
import java.util.concurrent.ArrayBlockingQueue
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

class GetOneEvent(
    private val sseResponseBuilder: SseResponseBuilder,
    private val keepAliveHandler: KeepAliveHandler,
    private val searchEventsHandler: SearchEventsHandler
) : NoSseServlet() {

    companion object {
        private val logger = KotlinLogging.logger { }
    }

    override fun doGet(req: HttpServletRequest, resp: HttpServletResponse) {
        val queue = ArrayBlockingQueue<SseEvent>(2)
        val eventId = req.pathInfo.run {
            if (startsWith('/')) {
                substring(1)
            } else {
                this
            }
        }

        val queryParametersMap = getParameters(req)
        logger.info { "Received get message request (${req.pathInfo}) with parameters: $queryParametersMap" }

        val reqContext = HttpEventResponseHandler(queue, sseResponseBuilder)
        keepAliveHandler.addKeepAliveData(reqContext).use {
            try {
                val toEventIds = toEventIds(eventId)
                val request = GetEventRequest(toEventIds.first, toEventIds.second)

                searchEventsHandler.loadOneEvent(request, reqContext)
            } catch (ex: Exception) {
                logger.error(ex) { "error getting event $eventId" }
                reqContext.writeErrorMessage(ex.message ?: ex.toString())
                reqContext.complete()
            }

            this.waitAndWrite(queue, resp)
        }
        logger.info { "Processing search sse events request finished" }
    }

    private fun toEventIds(evId: String): Pair<String?, String> {
        if (!evId.contains('/') && !evId.contains('?')) {
            val split = evId.split(ProviderEventId.DIVIDER)
            if (split.size == 2) {
                return split[0] to split[1]
            } else if (split.size == 1) {
                return null to split[0]
            }
        }
        throw IllegalArgumentException("Invalid event id: $evId")
    }

}