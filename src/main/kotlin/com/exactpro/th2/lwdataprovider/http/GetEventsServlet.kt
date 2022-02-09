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

import com.exactpro.th2.lwdataprovider.SseResponseHandler
import com.exactpro.th2.lwdataprovider.workers.KeepAliveHandler
import com.exactpro.th2.lwdataprovider.SseEvent
import com.exactpro.th2.lwdataprovider.SseResponseBuilder
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import com.exactpro.th2.lwdataprovider.entities.filters.PredicateFactory
import com.exactpro.th2.lwdataprovider.entities.filters.events.EventBodyFilter
import com.exactpro.th2.lwdataprovider.entities.filters.events.EventNameFilter
import com.exactpro.th2.lwdataprovider.entities.filters.events.EventStatusFilter
import com.exactpro.th2.lwdataprovider.entities.filters.events.EventTypeFilter
import com.exactpro.th2.lwdataprovider.entities.requests.SseEventSearchRequest
import com.exactpro.th2.lwdataprovider.entities.responses.BaseEventEntity
import com.exactpro.th2.lwdataprovider.handlers.SearchEventsHandler
import com.fasterxml.jackson.databind.ObjectMapper
import mu.KotlinLogging
import java.util.concurrent.ArrayBlockingQueue
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

class GetEventsServlet
    (private val configuration: Configuration, private val jacksonMapper: ObjectMapper,
     private val keepAliveHandler: KeepAliveHandler,
     private val searchEventsHandler: SearchEventsHandler
     )
    : SseServlet() {

    val eventFiltersPredicateFactory: PredicateFactory<BaseEventEntity> = PredicateFactory(
        mapOf(
            EventTypeFilter.filterInfo to EventTypeFilter.Companion::build,
            EventNameFilter.filterInfo to EventNameFilter.Companion::build,
            EventBodyFilter.filterInfo to EventBodyFilter.Companion::build,
            EventStatusFilter.filterInfo to EventStatusFilter.Companion::build
        )
    )

    companion object {
        private val logger = KotlinLogging.logger { }
    }
    
    override fun doGet(req: HttpServletRequest?, resp: HttpServletResponse?) {
        
        checkNotNull(req)
        checkNotNull(resp)

        val queryParametersMap = getParameters(req)
        logger.info { "Received search sse event request with parameters: $queryParametersMap" }

        val filterPredicate =
            eventFiltersPredicateFactory.build(queryParametersMap)
        val request = SseEventSearchRequest(queryParametersMap, filterPredicate)
        request.checkRequest()
        
        val queue = ArrayBlockingQueue<SseEvent>(configuration.responseQueueSize)
        val sseResponseBuilder = SseResponseHandler(queue, SseResponseBuilder(jacksonMapper))
        val reqContext = SseEventRequestContext(sseResponseBuilder, queryParametersMap)
        keepAliveHandler.addKeepAliveData(reqContext)
        searchEventsHandler.loadEvents(request, reqContext)

        this.waitAndWrite(queue, resp, reqContext)
        logger.info { "Processing search sse events request finished" }
        keepAliveHandler.removeKeepAliveData(reqContext)
    }
    

}