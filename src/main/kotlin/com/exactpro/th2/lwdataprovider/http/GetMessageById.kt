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

import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.lwdataprovider.ExceptionInfo
import com.exactpro.th2.lwdataprovider.SseEvent
import com.exactpro.th2.lwdataprovider.SseResponseBuilder
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import com.exactpro.th2.lwdataprovider.db.DataMeasurement
import com.exactpro.th2.lwdataprovider.entities.requests.GetMessageRequest
import com.exactpro.th2.lwdataprovider.entities.responses.ProviderMessage53
import com.exactpro.th2.lwdataprovider.failureReason
import com.exactpro.th2.lwdataprovider.handlers.SearchMessagesHandler
import io.javalin.Javalin
import io.javalin.http.Context
import io.javalin.http.pathParamAsClass
import io.javalin.http.queryParamAsClass
import io.javalin.openapi.HttpMethod
import io.javalin.openapi.OpenApi
import io.javalin.openapi.OpenApiContent
import io.javalin.openapi.OpenApiParam
import io.javalin.openapi.OpenApiResponse
import mu.KotlinLogging
import java.util.concurrent.ArrayBlockingQueue
import java.util.function.Supplier

class GetMessageById(
    private val configuration: Configuration,
    private val sseResponseBuilder: SseResponseBuilder,
    private val searchMessagesHandler: SearchMessagesHandler,
    private val dataMeasurement: DataMeasurement,
) : AbstractRequestHandler() {

    companion object {
        const val ROUTE = "/message/{id}"
        private val logger = KotlinLogging.logger { }
    }

    override fun setup(app: Javalin) {
        app.get(ROUTE, this)
    }


    @OpenApi(
        path = ROUTE,
        methods = [HttpMethod.GET],
        description = "returns message with the requested id",
        pathParams = [
            OpenApiParam(
                name = "id",
                required = true,
                description = "requested message ID",
                example = "book:session_alias:1:20221031130000000000000:1",
            )
        ],
        queryParams = [
            OpenApiParam("onlyRaw", type = Boolean::class,
                description = "only raw message will be returned in the response"),
        ],
        responses = [
            OpenApiResponse(
                status = "200",
                content = [
                    OpenApiContent(from = ProviderMessage53::class)
                ],
                description = "the message associated with specified id"
            ),
            OpenApiResponse(
                status = "404",
                content = [
                    OpenApiContent(from = ExceptionInfo::class)
                ],
                description = "messages is not found",
            )
        ]
    )
    override fun handle(ctx: Context) {
        val queue = ArrayBlockingQueue<Supplier<SseEvent>>(2)
        val msgId = ctx.pathParamAsClass<String>("id").get()
        val onlyRaw = ctx.queryParamAsClass<Boolean>("onlyRaw")
            .getOrDefault(false)


        val handler = HttpMessagesRequestHandler(queue, sseResponseBuilder, dataMeasurement)
        try {
            val newMsgId = parseMessageId(msgId)
            logger.info { "Received message request with id $msgId (onlyRaw: $onlyRaw)" }

            val request = GetMessageRequest(newMsgId, onlyRaw)

            searchMessagesHandler.loadOneMessage(request, handler, dataMeasurement)

            ctx.waitAndWrite(queue) { newMsgId.failureReason(it) }
        } catch (ex: Exception) {
            logger.error(ex) { "cannot load message $msgId" }
            handler.writeErrorMessage(ex.message ?: ex.toString())
            handler.complete()
        } finally {
            logger.info { "Processing message request finished" }
        }
    }

    private fun parseMessageId(msgId: String): StoredMessageId = try {
        StoredMessageId.fromString(msgId)
    } catch (ex: Exception) {
        throw IllegalArgumentException("Invalid message id: $msgId", ex)
    }


}