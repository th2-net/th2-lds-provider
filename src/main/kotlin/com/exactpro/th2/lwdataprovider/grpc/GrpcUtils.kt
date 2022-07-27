/*******************************************************************************
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
 ******************************************************************************/

package com.exactpro.th2.lwdataprovider.grpc

import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.grpc.ConnectionID
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.dataprovider.grpc.MessageStream
import com.exactpro.th2.dataprovider.grpc.TimeRelation
import com.exactpro.th2.lwdataprovider.entities.requests.ProviderMessageStream
import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp
import java.time.Instant

fun Timestamp.toInstant() : Instant = Instant.ofEpochSecond(this.seconds, this.nanos.toLong())

fun TimeRelation?.toProviderRelation(): com.exactpro.cradle.TimeRelation {
    return if (this == null || this == TimeRelation.NEXT)
        com.exactpro.cradle.TimeRelation.AFTER
    else
        com.exactpro.cradle.TimeRelation.BEFORE
}

fun Direction.toCradleDirection(): com.exactpro.cradle.Direction {
    return if (this == Direction.FIRST)
        com.exactpro.cradle.Direction.FIRST
    else
        com.exactpro.cradle.Direction.SECOND
}

fun com.exactpro.cradle.Direction.toGrpcDirection(): Direction {
    return if (this == com.exactpro.cradle.Direction.FIRST)
        Direction.FIRST
    else
        Direction.SECOND
}

fun MessageID.toStoredMessageId(): StoredMessageId {
    return StoredMessageId(this.connectionId.sessionAlias, this.direction.toCradleDirection(), this.sequence)
}

fun StoredMessageId.toGrpcMessageId(): MessageID {
    return MessageID.newBuilder().apply {
        this.connectionId = ConnectionID.newBuilder().setSessionAlias(this@toGrpcMessageId.streamName).build()
        this.direction = this@toGrpcMessageId.direction.toGrpcDirection()
        this.sequence = this@toGrpcMessageId.index
    }.build()
}

fun MessageStream.toProviderMessageStreams(): ProviderMessageStream {
    return ProviderMessageStream(this.name, this.direction.toCradleDirection())
}

fun StoredMessage.toRawMessage(): RawMessage {
    val message = this
    return RawMessage.newBuilder().apply {
        metadataBuilder.apply {
            putAllProperties(message.metadata?.toMap() ?: emptyMap())
            id = message.id.toGrpcMessageId()
            timestamp = message.timestamp.toTimestamp()
            protocol = message.protocol ?: ""
        }.build()
        body = ByteString.copyFrom(message.content)
    }.build()
}