/*
 * Copyright 2023-2024 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.lwdataprovider.ExceptionInfo
import com.exactpro.th2.lwdataprovider.SseEvent
import com.exactpro.th2.lwdataprovider.SseResponseBuilder
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import com.exactpro.th2.lwdataprovider.db.DataMeasurement
import com.exactpro.th2.lwdataprovider.entities.internal.ResponseFormat
import com.exactpro.th2.lwdataprovider.entities.requests.GetGroupMessageRequest
import com.exactpro.th2.lwdataprovider.entities.responses.ProviderMessage53
import com.exactpro.th2.lwdataprovider.failureReason
import com.exactpro.th2.lwdataprovider.handlers.SearchMessagesHandler
import io.javalin.Javalin
import io.javalin.http.Context
import io.javalin.http.pathParamAsClass
import io.javalin.openapi.HttpMethod
import io.javalin.openapi.OpenApi
import io.javalin.openapi.OpenApiContent
import io.javalin.openapi.OpenApiParam
import io.javalin.openapi.OpenApiResponse
import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.Executor
import java.util.function.Supplier

class GetSingleMessageByGroupAndId(
    private val searchHandler: SearchMessagesHandler,
    private val configuration: Configuration,
    private val sseResponseBuilder: SseResponseBuilder,
    private val convExecutor: Executor,
    private val dataMeasurement: DataMeasurement,
) : AbstractRequestHandler() {
    override fun setup(app: Javalin, context: JavalinContext) {
        app.get(ROUTE, this)
    }

    @OpenApi(
        path = ROUTE,
        methods = [HttpMethod.GET],
        pathParams = [
            OpenApiParam(
                name = GROUP_PARAM,
                description = "group name",
                required = true,
                example = "group42",
            ),
            OpenApiParam(
                name = ID_PARAM,
                description = "message ID to search",
                required = true,
                example = "book1:alias:1:20221031130000000000000:42",
            )
        ],
        queryParams = [
            OpenApiParam(
                name = RESPONSE_FORMAT,
                type = Array<ResponseFormat>::class,
                description = "expected response formats to receive"
            )
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
        val msgId = ctx.pathParamAsClass<String>(ID_PARAM).get()
        val groupName = ctx.pathParamAsClass<String>(GROUP_PARAM).get()
        val responseFormats = ctx.queryParams(RESPONSE_FORMAT)
            .takeUnless { it.isEmpty() }
            ?.map(ResponseFormat.Companion::fromString)?.toSet()
            ?: configuration.responseFormats


        val handler = HttpMessagesRequestHandler(
            queue, sseResponseBuilder, convExecutor,
            dataMeasurement,
            responseFormats = responseFormats,
        )
        val newMsgId: StoredMessageId = parseMessageId(msgId)
        try {
            LOGGER.info { "Received message request with id $msgId for group $groupName (responseFormats: $responseFormats)" }

            val request = GetGroupMessageRequest(
                group = groupName,
                messageId = newMsgId,
                rawOnly = responseFormats.run { size == 1 && contains(ResponseFormat.BASE_64) },
            )

            searchHandler.loadOneMessageByGroup(request, handler, dataMeasurement)
        } catch (ex: Exception) {
            LOGGER.error(ex) { "cannot load message $msgId for group $groupName" }
            handler.writeErrorMessage(ex.message ?: ex.toString())
            handler.complete()
        }
        ctx.waitAndWrite(queue) { newMsgId.failureReason(it) }
        LOGGER.info { "Processing message request finished" }
    }

    companion object {
        private const val GROUP_PARAM = "group"
        private const val ID_PARAM = "id"
        private const val RESPONSE_FORMAT = "responseFormat"
        private val LOGGER = KotlinLogging.logger {}

        const val ROUTE = "/message/group/{$GROUP_PARAM}/{$ID_PARAM}"

        @JvmStatic
        private fun parseMessageId(msgId: String): StoredMessageId = try {
            StoredMessageId.fromString(msgId)
        } catch (ex: Exception) {
            throw IllegalArgumentException("Invalid message id: $msgId", ex)
        }
    }
}