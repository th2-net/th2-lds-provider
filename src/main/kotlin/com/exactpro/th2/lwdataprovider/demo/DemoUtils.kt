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

package com.exactpro.th2.lwdataprovider.demo

import com.exactpro.cradle.BookId
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoDirection
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoMessageId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoParsedMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoRawMessage
import io.netty.buffer.Unpooled

fun DemoDirection.toCradleDirection(): com.exactpro.cradle.Direction {
    return if (this == DemoDirection.INCOMING)
        com.exactpro.cradle.Direction.FIRST
    else
        com.exactpro.cradle.Direction.SECOND
}

fun DemoDirection.toProtoDirection(): Direction {
    return if (this == DemoDirection.INCOMING)
        Direction.FIRST
    else
        Direction.SECOND
}

fun com.exactpro.cradle.Direction.toDemoDirection(): DemoDirection {
    return if (this == com.exactpro.cradle.Direction.FIRST)
        DemoDirection.INCOMING
    else
        DemoDirection.OUTGOING
}

fun DemoMessageId.toProtoMessageId(): MessageID = MessageID.newBuilder().apply {
    connectionIdBuilder.apply {
        this.sessionGroup = this@toProtoMessageId.sessionGroup
        this.sessionAlias = this@toProtoMessageId.sessionAlias
    }
    this.bookName = this@toProtoMessageId.book
    this.direction = this@toProtoMessageId.direction.toProtoDirection()
    this.timestamp = this@toProtoMessageId.timestamp.toTimestamp()
    this.sequence = this@toProtoMessageId.sequence
    this@toProtoMessageId.subsequence.forEach {
        this.addSubsequence(it.toInt())
    }
}.build()

fun StoredMessageId.toDemoMessageId(): DemoMessageId {
    return DemoMessageId(
        bookId.name,
        sessionAlias = sessionAlias,
        direction = direction.toDemoDirection(),
        sequence = sequence,
        timestamp = timestamp
    )
}

fun StoredMessage.toDemoRawMessage(): DemoRawMessage {
    return DemoRawMessage(
        id.toDemoMessageId(),
        null,
        metadata.toMap(),
        protocol ?: "",
        Unpooled.wrappedBuffer(content)
    )
}

fun DemoParsedMessage.toProtoMessage(): Message = Message.newBuilder().apply {
    metadataBuilder.apply {
        this.id = this@toProtoMessage.id.toProtoMessageId()
        this.protocol = this@toProtoMessage.protocol
        this.messageType = this@toProtoMessage.type
        if (this@toProtoMessage.metadata.isNotEmpty()) {
            this.putAllProperties(this@toProtoMessage.metadata)
        }
    }
    TODO("implement body convert")
}.build()