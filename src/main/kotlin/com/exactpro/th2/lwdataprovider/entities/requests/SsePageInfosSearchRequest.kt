/*
 * Copyright 2022 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.lwdataprovider.entities.requests

import com.exactpro.cradle.BookId
import com.exactpro.th2.lwdataprovider.entities.requests.util.invalidRequest
import java.time.Instant

class SsePageInfosSearchRequest(
    bookId: BookId?,
    startTimestamp: Instant?,
    endTimestamp: Instant?,
    val resultCountLimit: Int?,
) {

    val bookId: BookId
    val startTimestamp: Instant
    val endTimestamp: Instant

    init {
        this.bookId = bookId ?: invalidRequest("book id is not set")
        this.startTimestamp = startTimestamp ?: invalidRequest("start timestamp is not set")
        this.endTimestamp = endTimestamp ?: invalidRequest("end timestamp is not set")
        checkRequest()
    }

//    companion object {
//        private val httpConverter = HttpFilterConverter()
//        private val grpcConverter = GrpcFilterConverter()
//    }

    constructor(parameters: Map<String, List<String>>) : this(
        bookId = parameters["bookId"]?.firstOrNull()?.run(::BookId)
            ?: invalidRequest("parameter 'bookId' is required"),
        startTimestamp = parameters["startTimestamp"]?.firstOrNull()?.let { Instant.ofEpochMilli(it.toLong()) },
        endTimestamp = parameters["endTimestamp"]?.firstOrNull()?.let { Instant.ofEpochMilli(it.toLong()) },
        resultCountLimit = parameters["resultCountLimit"]?.firstOrNull()?.toInt(),
    )

    //TODO: Implement gRPC
//    constructor(request: EventSearchRequest) : this(
//        startTimestamp = if (request.hasStartTimestamp())
//            request.startTimestamp.let {
//                Instant.ofEpochSecond(it.seconds, it.nanos.toLong())
//            } else null,
//        parentEvent = if (request.hasParentEvent()) {
//            ProviderEventId(request.parentEvent.id)
//        } else null,
//        searchDirection = request.searchDirection.let {
//            when (it) {
//                PREVIOUS -> SearchDirection.previous
//                else -> SearchDirection.next
//            }
//        },
//        endTimestamp = if (request.hasEndTimestamp())
//            request.endTimestamp.let {
//                Instant.ofEpochSecond(it.seconds, it.nanos.toLong())
//            } else null,
//        resultCountLimit = if (request.hasResultCountLimit()) {
//            request.resultCountLimit.value
//        } else null,
//        filter = EventsFilterFactory.create(grpcConverter.convert(request.filterList)),
//        bookId = request.run { if (hasBookId()) bookId.toCradle() else invalidRequest("parameter 'bookId' is required") },
//        scope = request.run { if (hasScope()) scope.name else invalidRequest("parameter 'scope' is required") },
//    )

    private fun checkEndTimestamp() {
        if (startTimestamp.isAfter(endTimestamp)) {
            invalidRequest("startTimestamp: $startTimestamp > endTimestamp: $endTimestamp")
        }
    }

    private fun checkRequest() {
        checkEndTimestamp()
    }

    override fun toString(): String {
        return "SseEventSearchRequest(" +
                "bookId=$bookId, " +
                "startTimestamp=$startTimestamp, " +
                "endTimestamp=$endTimestamp, " +
                "resultCountLimit=$resultCountLimit" +
                ")"
    }


}
