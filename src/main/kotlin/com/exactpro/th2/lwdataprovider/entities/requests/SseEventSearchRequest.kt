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

import com.exactpro.cradle.TimeRelation
import com.exactpro.th2.dataprovider.lw.grpc.EventSearchRequest
import com.exactpro.th2.dataprovider.lw.grpc.TimeRelation.PREVIOUS
import com.exactpro.th2.lwdataprovider.entities.exceptions.InvalidRequestException
import com.exactpro.th2.lwdataprovider.entities.internal.ProviderEventId
import com.exactpro.th2.lwdataprovider.entities.requests.converter.GrpcFilterConverter
import com.exactpro.th2.lwdataprovider.entities.requests.converter.HttpFilterConverter
import com.exactpro.th2.lwdataprovider.entities.responses.BaseEventEntity
import com.exactpro.th2.lwdataprovider.filter.DataFilter
import com.exactpro.th2.lwdataprovider.filter.events.EventsFilterFactory
import java.time.Instant

class SseEventSearchRequest(
    val startTimestamp: Instant?,
    val parentEvent: ProviderEventId?,
    val searchDirection: TimeRelation,
    val resultCountLimit: Int?,
    endTimestamp: Instant?,
    val filter: DataFilter<BaseEventEntity> = DataFilter.acceptAll(),
) {

    val endTimestamp : Instant

    init {
        this.endTimestamp = getInitEndTimestamp(endTimestamp, resultCountLimit, searchDirection)
        checkRequest()
    }

    companion object {
        private val httpConverter = HttpFilterConverter()
        private val grpcConverter = GrpcFilterConverter()
        private fun asCradleTimeRelation(value: String): TimeRelation {
            if (value == "next") return TimeRelation.AFTER
            if (value == "previous") return TimeRelation.BEFORE

            throw InvalidRequestException("'$value' is not a valid timeline direction. Use 'next' or 'previous'")
        }
    }

    constructor(parameters: Map<String, List<String>>) : this(
        startTimestamp = parameters["startTimestamp"]?.firstOrNull()?.let { Instant.ofEpochMilli(it.toLong()) },
        parentEvent = parameters["parentEvent"]?.firstOrNull()?.let { ProviderEventId(it) },
        searchDirection = parameters["searchDirection"]?.firstOrNull()?.let {
            asCradleTimeRelation(it)
        } ?: TimeRelation.AFTER,
        endTimestamp = parameters["endTimestamp"]?.firstOrNull()?.let { Instant.ofEpochMilli(it.toLong()) },
        resultCountLimit = parameters["resultCountLimit"]?.firstOrNull()?.toInt(),
        filter = EventsFilterFactory.create(httpConverter.convert(parameters)),
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
                PREVIOUS -> TimeRelation.BEFORE
                else -> TimeRelation.AFTER
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
    )

    private fun checkEndTimestamp() {
        if (startTimestamp == null) return

        if (searchDirection == TimeRelation.AFTER) {
            if (startTimestamp.isAfter(endTimestamp))
                throw InvalidRequestException("startTimestamp: $startTimestamp > endTimestamp: $endTimestamp")
        } else {
            if (startTimestamp.isBefore(endTimestamp))
                throw InvalidRequestException("startTimestamp: $startTimestamp < endTimestamp: $endTimestamp")
        }
    }

    private fun checkStartPoint() {
        if (startTimestamp == null)
            throw InvalidRequestException("'startTimestamp' must not be null")
    }

    private fun checkRequest() {
        checkStartPoint()
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
                ")"
    }


}
