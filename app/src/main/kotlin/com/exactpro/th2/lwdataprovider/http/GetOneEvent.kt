/*
 * Copyright 2021-2024 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.lwdataprovider.ExceptionInfo
import com.exactpro.th2.lwdataprovider.SseEvent
import com.exactpro.th2.lwdataprovider.SseResponseBuilder
import com.exactpro.th2.lwdataprovider.db.DataMeasurement
import com.exactpro.th2.lwdataprovider.entities.requests.GetEventRequest
import com.exactpro.th2.lwdataprovider.entities.responses.Event
import com.exactpro.th2.lwdataprovider.failureReason
import com.exactpro.th2.lwdataprovider.handlers.SearchEventsHandler
import io.javalin.Javalin
import io.javalin.http.Context
import io.javalin.openapi.HttpMethod
import io.javalin.openapi.OpenApi
import io.javalin.openapi.OpenApiContent
import io.javalin.openapi.OpenApiParam
import io.javalin.openapi.OpenApiResponse
import io.github.oshai.kotlinlogging.KotlinLogging
import java.util.concurrent.ArrayBlockingQueue
import java.util.function.Supplier

class GetOneEvent(
    private val sseResponseBuilder: SseResponseBuilder,
    private val searchEventsHandler: SearchEventsHandler,
    private val dataMeasurement: DataMeasurement,
) : AbstractRequestHandler() {

    companion object {
        const val ROUTE = "/event/{id}"
        private val logger = KotlinLogging.logger { }
    }

    override fun setup(app: Javalin, context: JavalinContext) {
        app.get(ROUTE, this)
    }

    @OpenApi(
        path = ROUTE,
        methods = [HttpMethod.GET],
        description = "returns event with the requested id",
        pathParams = [
            OpenApiParam(
                name = "id",
                required = true,
                description = "requested event ID",
                example = "book:scope:20221031130000000000000:eventId",
            )
        ],
        responses = [
            OpenApiResponse(
                status = "200",
                content = [
                    OpenApiContent(from = Event::class)
                ],
            ),
            OpenApiResponse(
                status = "404",
                content = [
                    OpenApiContent(from = ExceptionInfo::class)
                ],
                description = "event is not found",
            )
        ]
    )
    override fun handle(ctx: Context) {
        val queue = ArrayBlockingQueue<Supplier<SseEvent>>(2)
        val eventId = ctx.pathParam("id")

        logger.info { "Received get event request ($eventId)" }

        val reqContext = HttpGenericResponseHandler(
            queue,
            sseResponseBuilder,
            Runnable::run,
            dataMeasurement,
            Event::eventId,
            SseResponseBuilder::build
        )
        var request: GetEventRequest? = null
        try {
            request = GetEventRequest.fromString(eventId)
            searchEventsHandler.loadOneEvent(request, reqContext)
        } catch (ex: Exception) {
            logger.error(ex) { "error getting event $eventId" }
            reqContext.writeErrorMessage(ex.message ?: ex.toString())
            reqContext.complete()
        } finally {
            logger.info { "Processing search sse events request finished" }
        }

        ctx.waitAndWrite(queue) { request?.failureReason(it) ?: it }
    }

}