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

import com.exactpro.cradle.BookId
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.grpc.ConnectionID
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.dataprovider.lw.grpc.MessageStream
import com.exactpro.th2.dataprovider.lw.grpc.PageInfoResponse
import com.exactpro.th2.dataprovider.lw.grpc.TimeRelation
import com.exactpro.th2.lwdataprovider.entities.requests.ProviderMessageStream
import com.exactpro.th2.lwdataprovider.entities.requests.SearchDirection
import com.exactpro.th2.lwdataprovider.entities.responses.PageId
import com.exactpro.th2.lwdataprovider.entities.responses.PageInfo
import com.google.protobuf.StringValue
import com.google.protobuf.Timestamp
import com.google.protobuf.UnsafeByteOperations
import java.time.Instant
import com.exactpro.th2.dataprovider.lw.grpc.PageId as GrpcPageId

fun Timestamp.toInstant() : Instant = Instant.ofEpochSecond(this.seconds, this.nanos.toLong())

fun TimeRelation?.toProviderRelation(): SearchDirection {
    return if (this == null || this == TimeRelation.NEXT)
        SearchDirection.next
    else
        SearchDirection.previous
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
    return StoredMessageId(BookId(bookName), connectionId.sessionAlias, direction.toCradleDirection(), timestamp.toInstant(), sequence)
}

fun StoredMessageId.toGrpcMessageId(): MessageID {
    return MessageID.newBuilder().apply {
        connectionId = ConnectionID.newBuilder().setSessionAlias(this@toGrpcMessageId.sessionAlias).build()
        this.direction = this@toGrpcMessageId.direction.toGrpcDirection()
        this.sequence = this@toGrpcMessageId.sequence
        this.timestamp = this@toGrpcMessageId.timestamp.toTimestamp()
        this.bookName = this@toGrpcMessageId.bookId.name
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
            protocol = message.protocol ?: ""
        }.build()
        body = UnsafeByteOperations.unsafeWrap(message.content)
    }.build()
}

fun BookId.toGrpc(): com.exactpro.th2.dataprovider.lw.grpc.BookId {
    return grpcBookId(name)
}

fun PageInfo.toGrpc(): PageInfoResponse {
    return PageInfoResponse.newBuilder()
        .setId(id.toGrpc())
        .setStarted(started.toTimestamp())
        .also { builder ->
            ended?.also { builder.ended = it.toTimestamp() }
            updated?.also { builder.updated = it.toTimestamp() }
            removed?.also { builder.removed = it.toTimestamp() }
            comment?.also { builder.comment = StringValue.of(it) }
        }
        .build()
}

private fun PageId.toGrpc(): GrpcPageId {
    return GrpcPageId.newBuilder()
        .setName(name)
        .setBookId(grpcBookId(book))
        .build()
}

private fun grpcBookId(name: String): com.exactpro.th2.dataprovider.lw.grpc.BookId =
    com.exactpro.th2.dataprovider.lw.grpc.BookId.newBuilder().setName(name).build()
