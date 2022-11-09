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

import com.exactpro.cradle.Direction
import com.exactpro.th2.lwdataprovider.EventType
import com.exactpro.th2.lwdataprovider.MessageRequestContext
import com.exactpro.th2.lwdataprovider.SseEvent
import com.exactpro.th2.lwdataprovider.SseResponseBuilder
import com.exactpro.th2.lwdataprovider.SseResponseHandler
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import com.exactpro.th2.lwdataprovider.entities.requests.GetMessageRequest
import com.exactpro.th2.lwdataprovider.handlers.SearchMessagesHandler
import com.exactpro.th2.lwdataprovider.workers.KeepAliveHandler
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.gson.Gson
import mu.KotlinLogging
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

class GetMessageById(
    private val configuration: Configuration,
    private val jacksonMapper: ObjectMapper,
    private val keepAliveHandler: KeepAliveHandler,
    private val searchMessagesHandler: SearchMessagesHandler
) : NoSseServlet() {

    companion object {
        private val logger = KotlinLogging.logger { }
    }


    override fun doGet(req: HttpServletRequest, resp: HttpServletResponse) {
        val queue = ArrayBlockingQueue<SseEvent>(2)
        var msgId = req.pathInfo
        if (msgId.startsWith('/'))
            msgId = msgId.substring(1)

        var reqContext: MessageRequestContext? = null
        val newMsgId = checkId(msgId, queue)
        if (newMsgId != null) {
            val queryParametersMap = getParameters(req)
            logger.info { "Received search sse event request with parameters: $queryParametersMap" }

            val request = GetMessageRequest(newMsgId, queryParametersMap)

            val sseResponseBuilder = SseResponseBuilder(jacksonMapper)
            val sseResponse = SseResponseHandler(queue, sseResponseBuilder)
            reqContext = MessageSseRequestContext(sseResponse)
            keepAliveHandler.addKeepAliveData(reqContext)
            searchMessagesHandler.loadOneMessage(request, reqContext)
        }

        this.waitAndWrite(queue, resp)
        reqContext?.let { keepAliveHandler.removeKeepAliveData(it) }
        logger.info { "Processing search sse messages request finished" }
    }

    private fun checkId(msgId: String, out: ArrayBlockingQueue<SseEvent>): String? {

        if (!msgId.contains('/') && !msgId.contains('?')) {
            val split = msgId.split(':')
            if (split.size == 3 && split[2].all { it.isDigit() }) {
                if (Direction.byLabel(split[1]) != null)
                    return msgId
                else if (Direction.byLabel(split[1].lowercase(Locale.getDefault())) != null)
                    return split[0] + ":" + split[1].lowercase(Locale.getDefault()) + ":" + split[2]
            }
        }

        logger.error("Invalid message id: $msgId")
        out.put(
            SseEvent(
                Gson().toJson(
                    Collections.singletonMap(
                        "message",
                        "Invalid message id: $msgId"
                    )
                ), EventType.ERROR
            )
        )
        out.put(SseEvent(event = EventType.CLOSE))
        return null
    }


}