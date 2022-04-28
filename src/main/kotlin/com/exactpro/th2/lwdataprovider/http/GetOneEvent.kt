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

import com.exactpro.th2.lwdataprovider.*
import com.exactpro.th2.lwdataprovider.workers.KeepAliveHandler
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import com.exactpro.th2.lwdataprovider.entities.requests.GetEventRequest
import com.exactpro.th2.lwdataprovider.handlers.SearchEventsHandler
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.gson.Gson
import mu.KotlinLogging
import java.util.Collections
import java.util.concurrent.ArrayBlockingQueue
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

class GetOneEvent
    (private val configuration: Configuration, private val jacksonMapper: ObjectMapper,
     private val keepAliveHandler: KeepAliveHandler,
     private val searchEventsHandler: SearchEventsHandler
     )
    : NoSseServlet() {
    
    companion object {
        private val logger = KotlinLogging.logger { }
    }
    
    override fun doGet(req: HttpServletRequest?, resp: HttpServletResponse?) {
        
        checkNotNull(req)
        checkNotNull(resp)

        val queue = ArrayBlockingQueue<SseEvent>(2)
        var eventId = req.pathInfo
        if (eventId.startsWith('/'))
            eventId = eventId.substring(1)

        val queryParametersMap = getParameters(req)
        logger.info { "Received get message request (${req.pathInfo}) with parameters: $queryParametersMap" }

        val toEventIds = toEventIds(eventId, queue)
        var reqContext:SseEventRequestContext? = null
        if (toEventIds != null) {
            val request = GetEventRequest(toEventIds.first, toEventIds.second, queryParametersMap )

            val sseResponseBuilder = SseResponseHandler(queue, SseResponseBuilder(jacksonMapper))
            reqContext = SseEventRequestContext(sseResponseBuilder)
            keepAliveHandler.addKeepAliveData(reqContext)
            searchEventsHandler.loadOneEvent(request, reqContext)
        }

        this.waitAndWrite(queue, resp)
        reqContext?.let { keepAliveHandler.removeKeepAliveData(it) }
        logger.info { "Processing search sse events request finished" }
    }
    
    private fun toEventIds(evId: String, out: ArrayBlockingQueue<SseEvent>) : Pair<String?, String>? {
        if (!evId.contains('/') && !evId.contains('?')) {
            val split = evId.split(':')
            if (split.size == 2) {
                return split[0] to split[1]
            } else if (split.size == 1) {
                return null to split[0]
            }
        }

        logger.error("Invalid event id: $evId")
        out.put(SseEvent(Gson().toJson(Collections.singletonMap("message", "Invalid event id: $evId")),
            EventType.ERROR))
        out.put(SseEvent(event = EventType.CLOSE))
        return null
    }

}