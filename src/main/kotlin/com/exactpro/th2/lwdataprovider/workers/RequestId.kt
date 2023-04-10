/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.exactpro.th2.lwdataprovider.workers

import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.messages.StoredMessageIdUtils
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoDirection
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoMessageId
import com.exactpro.th2.lwdataprovider.grpc.toInstant
import java.time.Instant

sealed class RequestId {
    protected abstract val bookName: String
    protected abstract val sessionAlias: String
    protected abstract val directionNum: Int
    protected abstract val epochSecond: Long
    protected abstract val nano: Int
    protected abstract val sequence: Long

    protected abstract val timestamp: Instant

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is RequestId) return false

        if (bookName != other.bookName) return false
        if (sessionAlias != other.sessionAlias) return false
        if (directionNum != other.directionNum) return false
        if (epochSecond != other.epochSecond) return false
        if (nano != other.nano) return false
        if (sequence != other.sequence) return false

        return true
    }

    override fun hashCode(): Int {
        var result = bookName.hashCode()
        result = 31 * result + sessionAlias.hashCode()
        result = 31 * result + directionNum
        result = 31 * result + epochSecond.hashCode()
        result = 31 * result + nano
        result = 31 * result + sequence.hashCode()
        return result
    }

    override fun toString(): String {
        return "$bookName:$sessionAlias:$directionNum:${StoredMessageIdUtils.timestampToString(timestamp)}:$sequence"
    }
}

class ProtoRequestId(
    private val messageID: MessageID,
) : RequestId() {
    override val bookName: String
        get() = messageID.bookName
    override val sessionAlias: String
        get() = messageID.connectionId.sessionAlias
    override val directionNum: Int
        get() = if (messageID.direction == Direction.FIRST) 1 else 2
    override val epochSecond: Long
        get() = messageID.timestamp.seconds
    override val nano: Int
        get() = messageID.timestamp.nanos
    override val timestamp: Instant
        get() = messageID.timestamp.toInstant()
    override val sequence: Long
        get() = messageID.sequence
}

class DemoRequestId(
    private val messageID: DemoMessageId,
) : RequestId() {
    override val bookName: String
        get() = messageID.book
    override val sessionAlias: String
        get() = messageID.sessionAlias
    override val directionNum: Int
        get() = if (messageID.direction == DemoDirection.INCOMING) 1 else 2
    override val epochSecond: Long
        get() = messageID.timestamp.epochSecond
    override val nano: Int
        get() = messageID.timestamp.nano
    override val timestamp: Instant
        get() = messageID.timestamp
    override val sequence: Long
        get() = messageID.sequence
}

class CradleRequestId(
    private val messageID: StoredMessageId
) : RequestId() {
    override val bookName: String
        get() = messageID.bookId.name
    override val sessionAlias: String
        get() = messageID.sessionAlias
    override val directionNum: Int
        get() = if(messageID.direction == com.exactpro.cradle.Direction.FIRST) 1 else 2
    override val epochSecond: Long
        get() = messageID.timestamp.epochSecond
    override val nano: Int
        get() = messageID.timestamp.nano
    override val sequence: Long
        get() = messageID.sequence
    override val timestamp: Instant
        get() = messageID.timestamp

}