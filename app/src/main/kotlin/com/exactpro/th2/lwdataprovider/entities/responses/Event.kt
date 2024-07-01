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

import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.event.EventUtils
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.EventStatus.FAILED
import com.exactpro.th2.common.grpc.EventStatus.SUCCESS
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.dataprovider.lw.grpc.EventResponse
import com.exactpro.th2.lwdataprovider.entities.internal.ProviderEventId
import com.exactpro.th2.lwdataprovider.grpc.toGrpcMessageId
import com.google.protobuf.UnsafeByteOperations
import io.javalin.openapi.OpenApiPropertyType
import java.time.Instant

data class Event(
    //** full event id  */
    val eventId: String,
    val batchId: String?,
    /** last part of event id. it doesn't consider in equal and hashCode methods */
    val shortEventId: String,
    val isBatched: Boolean,
    val eventName: String,
    val eventType: String?,
    val endTimestamp: Instant?,
    val startTimestamp: Instant,
    @get:OpenApiPropertyType(definedBy = String::class)
    val parentEventId: ProviderEventId?,
    val successful: Boolean,
    val bookId: String,
    val scope: String,
    val attachedMessageIds: Set<String>,

    @get:OpenApiPropertyType(definedBy = Array<Any>::class)
    val body: ByteArray?
) {

    fun convertToGrpcEventData(): EventResponse {
        return EventResponse.newBuilder()
            .setEventId(EventUtils.toEventID(startTimestamp, bookId, scope, shortEventId))
            .setIsBatched(isBatched)
            .setEventName(eventName)
            .setStartTimestamp(startTimestamp.toTimestamp())
            .setStatus(if (successful) SUCCESS else FAILED)
            .addAllAttachedMessageId(convertMessageIdToProto(attachedMessageIds))
            .setBody(UnsafeByteOperations.unsafeWrap(body))
            .also { builder ->
                batchId?.let { builder.setBatchId(EventID.newBuilder().setId(it)) }
                eventType?.let { builder.setEventType(it) }
                endTimestamp?.let { builder.setEndTimestamp(it.toTimestamp()) }
                parentEventId?.let { builder.parentEventId = convertToEventIdProto(it) }
            }.build()
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Event

        if (eventId != other.eventId) return false
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
        if (!body.contentEquals(other.body)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = eventId.hashCode()
        result = 31 * result + (batchId?.hashCode() ?: 0)
        result = 31 * result + isBatched.hashCode()
        result = 31 * result + eventName.hashCode()
        result = 31 * result + (eventType?.hashCode() ?: 0)
        result = 31 * result + (endTimestamp?.hashCode() ?: 0)
        result = 31 * result + startTimestamp.hashCode()
        result = 31 * result + (parentEventId?.hashCode() ?: 0)
        result = 31 * result + successful.hashCode()
        result = 31 * result + bookId.hashCode()
        result = 31 * result + scope.hashCode()
        result = 31 * result + attachedMessageIds.hashCode()
        result = 31 * result + body.contentHashCode()
        return result
    }

    companion object {
        @JvmStatic
        fun convertToEventIdProto(id: ProviderEventId): EventID {
            return with(id.eventId) {
                EventUtils.toEventID(startTimestamp, bookId.name, scope, this.id)
            }
        }
        @JvmStatic
        fun convertMessageIdToProto(attachedMessageIds: Set<String>): List<MessageID> {
            return attachedMessageIds.map { id ->
                StoredMessageId.fromString(id).toGrpcMessageId()
            }
        }
    }
}
