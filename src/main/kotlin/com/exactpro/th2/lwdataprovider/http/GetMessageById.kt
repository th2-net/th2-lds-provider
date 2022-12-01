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
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.messages.StoredMessageIdUtils
import com.exactpro.th2.lwdataprovider.SseEvent
import com.exactpro.th2.lwdataprovider.SseResponseBuilder
import com.exactpro.th2.lwdataprovider.db.DataMeasurement
import com.exactpro.th2.lwdataprovider.entities.requests.GetMessageRequest
import com.exactpro.th2.lwdataprovider.handlers.SearchMessagesHandler
import com.exactpro.th2.lwdataprovider.workers.KeepAliveHandler
import mu.KotlinLogging
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

class GetMessageById(
    private val sseResponseBuilder: SseResponseBuilder,
    private val keepAliveHandler: KeepAliveHandler,
    private val searchMessagesHandler: SearchMessagesHandler,
    private val dataMeasurement: DataMeasurement,
) : NoSseServlet() {

    companion object {
        private val logger = KotlinLogging.logger { }
    }


    override fun doGet(req: HttpServletRequest, resp: HttpServletResponse) {
        val queue = ArrayBlockingQueue<SseEvent>(2)
        var msgId = req.pathInfo
        if (msgId.startsWith('/'))
            msgId = msgId.substring(1)


        val handler = HttpMessagesRequestHandler(queue, sseResponseBuilder, dataMeasurement)
        keepAliveHandler.addKeepAliveData(handler).use {
            try {
                val newMsgId = parseMessageId(msgId)
                val queryParametersMap = getParameters(req)
                logger.info { "Received search sse event request with parameters: $queryParametersMap" }

                val request = GetMessageRequest(newMsgId, queryParametersMap)

                searchMessagesHandler.loadOneMessage(request, handler, dataMeasurement)
            } catch (ex: Exception) {
                logger.error(ex) { "cannot load message $msgId" }
                handler.writeErrorMessage(ex.message ?: ex.toString())
                handler.complete()
            }

            this.waitAndWrite(queue, resp)
            logger.info { "Processing search sse messages request finished" }
        }
    }

    private fun parseMessageId(msgId: String): StoredMessageId = try {
        StoredMessageId.fromString(msgId)
    } catch (ex: Exception) {
        throw IllegalArgumentException("Invalid message id: $msgId", ex)
    }


}