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
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.dataprovider.grpc.MessageSearchRequest
import com.exactpro.th2.lwdataprovider.entities.exceptions.InvalidRequestException
import com.exactpro.th2.lwdataprovider.grpc.toInstant
import com.exactpro.th2.lwdataprovider.grpc.toProviderRelation
import com.exactpro.th2.lwdataprovider.grpc.toStoredMessageId
import java.time.Instant
import kotlin.streams.toList

data class SseMessageSearchRequest(
    val startTimestamp: Instant?,
    val stream: List<String>?,
    val searchDirection: TimeRelation,
    val endTimestamp: Instant?,
    val resumeFromId: StoredMessageId?,
    val resultCountLimit: Int?,
    val keepOpen: Boolean,
    val attachedEvents: Boolean,
    val lookupLimitDays: Int?,
    val resumeFromIdsList: List<StoredMessageId>?,
    val onlyRaw: Boolean
) {

    companion object {
        private fun asCradleTimeRelation(value: String): TimeRelation {
            if (value == "next") return TimeRelation.AFTER
            if (value == "previous") return TimeRelation.BEFORE

            throw InvalidRequestException("'$value' is not a valid timeline direction. Use 'next' or 'previous'")
        }
    }

    constructor(parameters: Map<String, List<String>>) : this(
        startTimestamp = parameters["startTimestamp"]?.firstOrNull()?.let { Instant.ofEpochMilli(it.toLong()) },
        stream = parameters["stream"],
        searchDirection = parameters["searchDirection"]?.firstOrNull()?.let {
            asCradleTimeRelation(
                it
            )
        } ?: TimeRelation.AFTER,
        endTimestamp = parameters["endTimestamp"]?.firstOrNull()?.let { Instant.ofEpochMilli(it.toLong()) },
        resumeFromId = parameters["resumeFromId"]?.firstOrNull().let { StoredMessageId.fromString(it) },
        resumeFromIdsList = parameters["messageId"]?.map { StoredMessageId.fromString(it) },
        resultCountLimit = parameters["resultCountLimit"]?.firstOrNull()?.toInt(),
        keepOpen = parameters["keepOpen"]?.firstOrNull()?.toBoolean() ?: false,
        attachedEvents = parameters["attachedEvents"]?.firstOrNull()?.toBoolean() ?: false,
        lookupLimitDays = parameters["lookupLimitDays"]?.firstOrNull()?.toInt(),
        onlyRaw = parameters["onlyRaw"]?.firstOrNull()?.toBoolean() ?: false
    )

    constructor(grpcRequest: MessageSearchRequest) : this(
        startTimestamp = grpcRequest.startTimestamp?.toInstant(),
        stream = grpcRequest.stream.listStringList,
        searchDirection = grpcRequest.searchDirection.toProviderRelation(),
        endTimestamp = grpcRequest.endTimestamp?.toInstant(),
        resumeFromId = if (grpcRequest.hasResumeFromId()) grpcRequest.resumeFromId.toStoredMessageId() else null,
        resumeFromIdsList = grpcRequest.messageIdList.stream().map { it.toStoredMessageId() }.toList(),
        resultCountLimit = if (grpcRequest.hasResultCountLimit()) grpcRequest.resultCountLimit.value else null,
        keepOpen = if (grpcRequest.hasKeepOpen()) grpcRequest.keepOpen.value else false,
        attachedEvents = false, // disabled
        lookupLimitDays = null,
        onlyRaw = false // NOT SUPPORTED in GRPC
    )

    private fun checkEndTimestamp() {
        if (endTimestamp == null || startTimestamp == null) return

        if (searchDirection == TimeRelation.AFTER) {
            if (startTimestamp.isAfter(endTimestamp))
                throw InvalidRequestException("startTimestamp: $startTimestamp > endTimestamp: $endTimestamp")
        } else {
            if (startTimestamp.isBefore(endTimestamp))
                throw InvalidRequestException("startTimestamp: $startTimestamp < endTimestamp: $endTimestamp")
        }
    }

    private fun checkStartPoint() {
        if (startTimestamp == null && resumeFromId == null && resumeFromIdsList == null)
            throw InvalidRequestException("One of the 'startTimestamp' or 'resumeFromId' or 'messageId' must not be null")
    }

    fun checkRequest() {
        checkStartPoint()
        checkEndTimestamp()
    }
}