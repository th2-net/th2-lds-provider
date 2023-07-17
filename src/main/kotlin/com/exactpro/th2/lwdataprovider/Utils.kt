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

package com.exactpro.th2.lwdataprovider

import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.messages.StoredMessageIdUtils
import com.exactpro.th2.dataprovider.lw.grpc.BookId
import com.exactpro.th2.lwdataprovider.entities.requests.GetEventRequest
import com.exactpro.th2.lwdataprovider.entities.responses.MessageIdWithGroup
import com.google.gson.Gson

private val gson = Gson()

fun BookId.toCradle(): com.exactpro.cradle.BookId = com.exactpro.cradle.BookId(name)

fun failureReason(batchId: String? = null, id: String? = null, error: String): String = gson.toJson(
    mapOf(
        "batchId" to batchId,
        "id" to id,
        "error" to error,
    )
)

fun StoredMessageId.toReportId() =
    "${bookId.name}:$sessionAlias:${direction.label}:${StoredMessageIdUtils.timestampToString(timestamp)}:$sequence"

fun MessageIdWithGroup.toReportId() = with(messageId) {
    "${bookId.name}:$group:$sessionAlias:${direction.label}:${StoredMessageIdUtils.timestampToString(timestamp)}:$sequence"
}
fun GetEventRequest.failureReason(error: String): String = failureReason(batchId, eventId, error)
fun StoredMessageId.failureReason(error: String): String = failureReason(
    null,
    toReportId(),
    error
)

fun MessageIdWithGroup.failureReason(error: String): String = failureReason(
    null,
    toReportId(),
    error
)