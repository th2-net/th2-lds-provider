/*
 * Copyright 2022-2023 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.dataprovider.lw.grpc.MessageGroupsSearchRequest
import com.exactpro.th2.dataprovider.lw.grpc.MessageStream
import com.exactpro.th2.lwdataprovider.entities.internal.ResponseFormat
import com.exactpro.th2.lwdataprovider.grpc.toInstant
import com.exactpro.th2.lwdataprovider.grpc.toProviderMessageStreams
import com.exactpro.th2.lwdataprovider.grpc.toProviderRelation
import com.exactpro.th2.lwdataprovider.toCradle
import java.time.Instant

data class MessagesGroupRequest(
    val groups: Set<String>,
    val startTimestamp: Instant,
    val endTimestamp: Instant,
    val keepOpen: Boolean,
    val bookId: BookId,
    val responseFormats: Set<ResponseFormat>? = null,
    val includeStreams: Set<ProviderMessageStream> = emptySet(),
    val limit: Int? = null,
    val searchDirection: SearchDirection = SearchDirection.next
) {
    init {
        if (!responseFormats.isNullOrEmpty()) {
            ResponseFormat.validate(responseFormats)
        }
    }
    companion object {
        private const val GROUP_PARAM = "group"
        private const val START_TIMESTAMP_PARAM = "startTimestamp"
        private const val END_TIMESTAMP_PARAM = "endTimestamp"
        private const val SORT_PARAMETER = "sort"
        private const val RAW_ONLY_PARAMETER = "onlyRaw"
        private const val KEEP_OPEN_PARAMETER = "keepOpen"
        private const val BOOK_ID_PARAM = "bookId"

        @JvmStatic
        fun fromGrpcRequest(request: MessageGroupsSearchRequest): MessagesGroupRequest = request.run {
            MessagesGroupRequest(
                messageGroupList.mapTo(HashSet(messageGroupCount)) {
                    it.name.apply { check(isNotEmpty()) { "group name cannot be empty" } }
                },
                if (hasStartTimestamp()) startTimestamp.toInstant() else error("missing start timestamp"),
                if (hasEndTimestamp()) endTimestamp.toInstant() else error("missing end timestamp"),
                false, // FIXME: update gRPC
                if (hasBookId()) bookId.toCradle() else error("parameter '$BOOK_ID_PARAM' is required"),
                request.responseFormatsList.takeIf { it.isNotEmpty() }
                    ?.mapTo(hashSetOf(), ResponseFormat.Companion::fromString)
                    .let { formats ->
                        if (request.rawOnly) {
                            formats?.let { it + ResponseFormat.BASE_64 } ?: setOf(ResponseFormat.BASE_64)
                        } else {
                            formats
                        }
                    },
                request.streamList.asSequence().map(MessageStream::toProviderMessageStreams).toSet(),
                if (request.hasResultCountLimit()) request.resultCountLimit.value else null,
                request.searchDirection.toProviderRelation()
            )
        }
    }
}