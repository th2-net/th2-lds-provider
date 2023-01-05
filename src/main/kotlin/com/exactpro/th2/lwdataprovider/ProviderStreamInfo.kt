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

package com.exactpro.th2.lwdataprovider

import com.exactpro.cradle.Direction
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.dataprovider.grpc.MessageStream
import com.exactpro.th2.dataprovider.grpc.MessageStreamPointer
import com.exactpro.th2.lwdataprovider.grpc.toGrpcDirection
import com.exactpro.th2.lwdataprovider.grpc.toGrpcMessageId
import java.util.concurrent.ConcurrentHashMap

class ProviderStreamInfo {

    private val streams: MutableMap<String, StreamDetails> = ConcurrentHashMap()

    fun registerMessage(msg: StoredMessageId?) {
        if (msg == null)
            return
        streams.computeIfAbsent(msg.streamName + msg.direction.label) {
            StreamDetails(msg.streamName, msg.direction)
        }.msgId = msg
    }

    fun registerSession(streamName: String, direction: Direction) {
        streams.computeIfAbsent(streamName + direction.label) {StreamDetails(streamName, direction)}
    }


    fun toGrpc(): Collection<MessageStreamPointer> {
        return streams.values.asSequence().map { streamDetails ->
            MessageStreamPointer.newBuilder().apply {
                this.messageStream = MessageStream.newBuilder().apply {
                    this.name = streamDetails.streamName
                    this.direction = streamDetails.direction.toGrpcDirection()
                }.build()
                this.lastId = streamDetails.msgId.toGrpcMessageId()
            }.build()
        }.toCollection(ArrayList(streams.size))
    }

}

data class StreamDetails(val streamName: String, val direction: Direction,
                         var msgId: StoredMessageId = StoredMessageId(streamName, direction, 0L)
)