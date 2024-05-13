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

package com.exactpro.th2.lwdataprovider.entities.requests

import com.exactpro.cradle.BookId
import com.exactpro.th2.dataprovider.lw.grpc.EventSearchRequest
import com.exactpro.th2.dataprovider.lw.grpc.TimeRelation.*
import com.exactpro.th2.lwdataprovider.entities.internal.ProviderEventId
import com.exactpro.th2.lwdataprovider.entities.requests.converter.GrpcFilterConverter
import com.exactpro.th2.lwdataprovider.entities.requests.converter.HttpFilterConverter
import com.exactpro.th2.lwdataprovider.entities.requests.util.getInitEndTimestamp
import com.exactpro.th2.lwdataprovider.entities.requests.util.invalidRequest
import com.exactpro.th2.lwdataprovider.entities.responses.BaseEventEntity
import com.exactpro.th2.lwdataprovider.filter.DataFilter
import com.exactpro.th2.lwdataprovider.filter.events.EventsFilterFactory
import com.exactpro.th2.lwdataprovider.toCradle
import java.time.Instant

class SseEventSearchRequest(
    startTimestamp: Instant?,
    val parentEvent: ProviderEventId?,
    val searchDirection: SearchDirection,
    val resultCountLimit: Int?,
    endTimestamp: Instant?,
    val filter: DataFilter<BaseEventEntity> = DataFilter.acceptAll(),
    val bookId: BookId,
    val scope: String,
) {

    val startTimestamp: Instant
    val endTimestamp : Instant?

    init {
        this.startTimestamp = startTimestamp ?: invalidRequest("start timestamp is not set")
        this.endTimestamp = getInitEndTimestamp(endTimestamp, resultCountLimit)
        checkRequest()
    }

    companion object {
        private val httpConverter = HttpFilterConverter()
        private val grpcConverter = GrpcFilterConverter()
    }

    constructor(parameters: Map<String, List<String>>) : this(
        startTimestamp = parameters["startTimestamp"]?.firstOrNull()?.let { Instant.ofEpochMilli(it.toLong()) },
        parentEvent = parameters["parentEvent"]?.firstOrNull()?.let { ProviderEventId(it) },
        searchDirection = parameters["searchDirection"]?.firstOrNull()?.let(SearchDirection::valueOf) ?: SearchDirection.next,
        endTimestamp = parameters["endTimestamp"]?.firstOrNull()?.let { Instant.ofEpochMilli(it.toLong()) },
        resultCountLimit = parameters["resultCountLimit"]?.firstOrNull()?.toInt(),
        filter = EventsFilterFactory.create(httpConverter.convert(parameters)),
        bookId = parameters["bookId"]?.firstOrNull()?.run(::BookId)
            ?: invalidRequest("parameter 'bookId' is required"),
        scope = parameters["scope"]?.firstOrNull()
            ?: invalidRequest("parameter 'scope' is required"),
    )

    constructor(request: EventSearchRequest) : this(
        startTimestamp = if (request.hasStartTimestamp())
            request.startTimestamp.let {
                Instant.ofEpochSecond(it.seconds, it.nanos.toLong())
            } else null,
        parentEvent = if (request.hasParentEvent()) {
            ProviderEventId(request.parentEvent.id)
        } else null,
        searchDirection = request.searchDirection.let {
            when (it) {
                PREVIOUS -> SearchDirection.previous
                else -> SearchDirection.next
            }
        },
        endTimestamp = if (request.hasEndTimestamp())
            request.endTimestamp.let {
                Instant.ofEpochSecond(it.seconds, it.nanos.toLong())
            } else null,
        resultCountLimit = if (request.hasResultCountLimit()) {
            request.resultCountLimit.value
        } else null,
        filter = EventsFilterFactory.create(grpcConverter.convert(request.filterList)),
        bookId = request.run { if (hasBookId()) bookId.toCradle() else invalidRequest("parameter 'bookId' is required") },
        scope = request.run { if (hasScope()) scope.name else invalidRequest("parameter 'scope' is required") },
    )

    private fun checkEndTimestamp() {
        if (endTimestamp == null) return

        if (searchDirection == SearchDirection.next) {
            if (startTimestamp.isAfter(endTimestamp))
                invalidRequest("startTimestamp: $startTimestamp > endTimestamp: $endTimestamp")
        } else {
            if (startTimestamp.isBefore(endTimestamp))
                invalidRequest("startTimestamp: $startTimestamp < endTimestamp: $endTimestamp")
        }
    }

    private fun checkRequest() {
        checkEndTimestamp()
    }

    override fun toString(): String {
        return "SseEventSearchRequest(" +
                "startTimestamp=$startTimestamp, " +
                "endTimestamp=$endTimestamp" +
                "parentEvent=$parentEvent, " +
                "searchDirection=$searchDirection, " +
                "resultCountLimit=$resultCountLimit, " +
                "filter=$filter, " +
                "bookId=$bookId, " +
                "scope='$scope', " +
                ")"
    }


}
