/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.lwdataprovider.transport

import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.grpc.Direction as ProtoDirection
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage
import io.netty.buffer.Unpooled

fun Direction.toCradleDirection(): com.exactpro.cradle.Direction {
    return if (this == Direction.INCOMING)
        com.exactpro.cradle.Direction.FIRST
    else
        com.exactpro.cradle.Direction.SECOND
}

fun Direction.toProtoDirection(): ProtoDirection {
    return if (this == Direction.INCOMING)
        ProtoDirection.FIRST
    else
        ProtoDirection.SECOND
}

fun com.exactpro.cradle.Direction.toDirection(): Direction {
    return if (this == com.exactpro.cradle.Direction.FIRST)
        Direction.INCOMING
    else
        Direction.OUTGOING
}

fun MessageId.toProtoMessageId(book: String, sessionGroup: String): MessageID = MessageID.newBuilder().apply {
    connectionIdBuilder.apply {
        this.sessionGroup = sessionGroup
        this.sessionAlias = this@toProtoMessageId.sessionAlias
    }
    this.bookName = book
    this.direction = this@toProtoMessageId.direction.toProtoDirection()
    this.timestamp = this@toProtoMessageId.timestamp.toTimestamp()
    this.sequence = this@toProtoMessageId.sequence
    this@toProtoMessageId.subsequence.forEach {
        this.addSubsequence(it)
    }
}.build()

fun StoredMessageId.toMessageId(): MessageId {
    return MessageId(
        sessionAlias = sessionAlias,
        direction = direction.toDirection(),
        sequence = sequence,
        timestamp = timestamp
    )
}

fun StoredMessage.toRawMessage(): RawMessage {
    return RawMessage(
        id.toMessageId(),
        null,
        metadata.toMap(),
        protocol ?: "",
        Unpooled.wrappedBuffer(content)
    )
}

fun ParsedMessage.toProtoMessage(book: String, sessionGroup: String): Message = Message.newBuilder().apply {
    metadataBuilder.apply {
        this.id = this@toProtoMessage.id.toProtoMessageId(book, sessionGroup)
        this.protocol = this@toProtoMessage.protocol
        this.messageType = this@toProtoMessage.type
        if (this@toProtoMessage.metadata.isNotEmpty()) {
            this.putAllProperties(this@toProtoMessage.metadata)
        }
    }
    TODO("implement body convert")
}.build()