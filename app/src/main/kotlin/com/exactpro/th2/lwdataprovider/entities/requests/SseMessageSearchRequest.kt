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
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.dataprovider.lw.grpc.MessageSearchRequest
import com.exactpro.th2.dataprovider.lw.grpc.MessageStreamPointer
import com.exactpro.th2.lwdataprovider.entities.internal.ResponseFormat
import com.exactpro.th2.lwdataprovider.entities.requests.util.convertToMessageStreams
import com.exactpro.th2.lwdataprovider.entities.requests.util.getInitEndTimestamp
import com.exactpro.th2.lwdataprovider.entities.requests.util.invalidRequest
import com.exactpro.th2.lwdataprovider.grpc.toInstant
import com.exactpro.th2.lwdataprovider.grpc.toProviderMessageStreams
import com.exactpro.th2.lwdataprovider.grpc.toProviderRelation
import com.exactpro.th2.lwdataprovider.grpc.toStoredMessageId
import com.exactpro.th2.lwdataprovider.toCradle
import java.time.Instant

class SseMessageSearchRequest(
    val startTimestamp: Instant?,
    val stream: List<ProviderMessageStream>?,
    val searchDirection: SearchDirection,
    val resultCountLimit: Int?,
    val keepOpen: Boolean,
    val resumeFromIdsList: List<StoredMessageId>?,

    endTimestamp: Instant?,
    val responseFormats: Set<ResponseFormat>? = null,
    val bookId: BookId,
) {
    init {
        if (keepOpen) {
            requireNotNull(startTimestamp) { "the start timestamp must be specified if keep open is used" }
            requireNotNull(endTimestamp) { "the end timestamp must be specified if keep open is used" }
        }
        if (!responseFormats.isNullOrEmpty()) {
            ResponseFormat.validate(responseFormats)
        }
    }

    val endTimestamp : Instant?

    init {
        this.endTimestamp = getInitEndTimestamp(endTimestamp, resultCountLimit)
        checkRequest()
        if (resumeFromIdsList.isNullOrEmpty() && stream.isNullOrEmpty()) {
            invalidRequest("either stream or IDs to resume must be set")
        }
    }

    companion object {

        private fun toMessageIds(streamsPointers: List<MessageStreamPointer>?): List<StoredMessageId>? {
            if (streamsPointers == null)
                return null;
            val providerMsgIds = ArrayList<StoredMessageId>(streamsPointers.size)
            streamsPointers.forEach {
                if (it.hasLastId()) {
                    providerMsgIds.add(it.lastId.toStoredMessageId())
                }
            }
            return providerMsgIds
        }
    }

    constructor(parameters: Map<String, List<String>>) : this(
        startTimestamp = parameters["startTimestamp"]?.firstOrNull()?.let { Instant.ofEpochMilli(it.toLong()) },
        stream = parameters["stream"]?.let(::convertToMessageStreams),
        searchDirection = parameters["searchDirection"]?.firstOrNull()?.let(SearchDirection::valueOf) ?: SearchDirection.next,
        endTimestamp = parameters["endTimestamp"]?.firstOrNull()?.let { Instant.ofEpochMilli(it.toLong()) },
        resumeFromIdsList = parameters["messageId"]?.map { StoredMessageId.fromString(it) },
        resultCountLimit = parameters["resultCountLimit"]?.firstOrNull()?.toInt(),
        keepOpen = parameters["keepOpen"]?.firstOrNull()?.toBoolean() ?: false,
        responseFormats = parameters["responseFormats"]?.mapTo(hashSetOf(), ResponseFormat.Companion::fromString),
        bookId = parameters["bookId"]?.firstOrNull()?.let(::BookId) ?: invalidRequest("parameter 'bookId' is required"),
    )


    constructor(grpcRequest: MessageSearchRequest) : this(
        startTimestamp = if (grpcRequest.hasStartTimestamp()){
            grpcRequest.startTimestamp.toInstant()
        } else null,
        stream = grpcRequest.streamList.map { it.toProviderMessageStreams() },
        searchDirection = grpcRequest.searchDirection.toProviderRelation(),
        endTimestamp = if (grpcRequest.hasEndTimestamp()){
            grpcRequest.endTimestamp.toInstant()
        } else null,
        resumeFromIdsList = if (grpcRequest.streamPointerList.isNotEmpty()){
            toMessageIds(grpcRequest.streamPointerList)
        } else null,
        resultCountLimit = if (grpcRequest.hasResultCountLimit()) grpcRequest.resultCountLimit.value else null,
        keepOpen = if (grpcRequest.hasKeepOpen()) grpcRequest.keepOpen.value else false,
        responseFormats = grpcRequest.responseFormatsList.takeIf { it.isNotEmpty() }?.mapTo(hashSetOf(), ResponseFormat.Companion::fromString),
        bookId = grpcRequest.run { if (hasBookId()) bookId.toCradle() else invalidRequest("parameter 'bookId' is required") },
    )

    private fun checkEndTimestamp() {
        if (startTimestamp == null || endTimestamp == null) return

        if (searchDirection == SearchDirection.next) {
            if (startTimestamp.isAfter(endTimestamp))
                invalidRequest("startTimestamp: $startTimestamp > endTimestamp: $endTimestamp")
        } else {
            if (startTimestamp.isBefore(endTimestamp))
                invalidRequest("startTimestamp: $startTimestamp < endTimestamp: $endTimestamp")
        }
    }

    private fun checkStartPoint() {
        if (startTimestamp == null && resumeFromIdsList == null)
            invalidRequest("One of the 'startTimestamp' or 'resumeFromId' or 'messageId' must not be null")
    }

    private fun checkRequest() {
        checkStartPoint()
        checkEndTimestamp()
    }

    override fun toString(): String {
        return "SseMessageSearchRequest(" +
                "startTimestamp=$startTimestamp, " +
                "endTimestamp=$endTimestamp" +
                "stream=$stream, " +
                "searchDirection=$searchDirection, " +
                "resultCountLimit=$resultCountLimit, " +
                "keepOpen=$keepOpen, " +
                "resumeFromIdsList=$resumeFromIdsList, " +
                "responseFormats=$responseFormats, " +
                "bookId=$bookId, " +
                ")"
    }
}

