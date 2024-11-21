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

package com.exactpro.th2.lwdataprovider.entities.responses

import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.th2.lwdataprovider.entities.internal.ProviderEventId
import java.time.Instant

data class BaseEventEntity(
    val type: String = "event",
    val fullEventId: ProviderEventId,
    val batchId: StoredTestEventId?,
    val isBatched: Boolean,
    val eventName: String,
    val eventType: String,
    val endTimestamp: Instant?,
    val startTimestamp: Instant,

    val parentEventId: ProviderEventId?,
    val successful: Boolean,
    val bookId: String,
    val scope: String,
    val attachedMessageIds: Set<String> = emptySet(),
    val body: ByteArray? = null,
) {
    fun convertToEvent(): Event {
        return Event(
            batchId = batchId?.toString(),
            shortEventId = fullEventId.eventId.id,
            isBatched = batchId != null,
            eventId = fullEventId.toString(),
            eventName = eventName,
            eventType = eventType,
            startTimestamp = startTimestamp,
            endTimestamp = endTimestamp,
            parentEventId = parentEventId,
            successful = successful,
            bookId = bookId,
            scope = scope,
            attachedMessageIds = attachedMessageIds,
            body = body
        )
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as BaseEventEntity

        if (type != other.type) return false
        if (fullEventId != other.fullEventId) return false
        if (batchId != other.batchId) return false
        if (isBatched != other.isBatched) return false
        if (eventName != other.eventName) return false
        if (eventType != other.eventType) return false
        if (endTimestamp != other.endTimestamp) return false
        if (startTimestamp != other.startTimestamp) return false
        if (parentEventId != other.parentEventId) return false
        if (successful != other.successful) return false
        if (bookId != other.bookId) return false
        if (scope != other.scope) return false
        if (attachedMessageIds != other.attachedMessageIds) return false
        if (body != null) {
            if (other.body == null) return false
            if (!body.contentEquals(other.body)) return false
        } else if (other.body != null) return false

        return true
    }

    override fun hashCode(): Int {
        var result = type.hashCode()
        result = 31 * result + fullEventId.hashCode()
        result = 31 * result + (batchId?.hashCode() ?: 0)
        result = 31 * result + isBatched.hashCode()
        result = 31 * result + eventName.hashCode()
        result = 31 * result + eventType.hashCode()
        result = 31 * result + (endTimestamp?.hashCode() ?: 0)
        result = 31 * result + startTimestamp.hashCode()
        result = 31 * result + (parentEventId?.hashCode() ?: 0)
        result = 31 * result + successful.hashCode()
        result = 31 * result + bookId.hashCode()
        result = 31 * result + scope.hashCode()
        result = 31 * result + attachedMessageIds.hashCode()
        result = 31 * result + (body?.contentHashCode() ?: 0)
        return result
    }
}
