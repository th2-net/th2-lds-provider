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
import com.exactpro.th2.dataprovider.grpc.MessageSearchResponse
import com.exactpro.th2.dataprovider.grpc.MessageStreamPointers
import com.exactpro.th2.lwdataprovider.GrpcResponseHandler
import com.exactpro.th2.lwdataprovider.MessageRequestContext
import com.exactpro.th2.lwdataprovider.RequestedMessageDetails
import com.exactpro.th2.lwdataprovider.producers.GrpcMessageProducer
import mu.KotlinLogging


class GrpcMessageRequestContext(
    override val channelMessages: GrpcResponseHandler,
    maxMessagesPerRequest: Int = 0
) : MessageRequestContext(channelMessages, maxMessagesPerRequest) {


    override fun createMessageDetails(id: String, storedMessage: StoredMessage, onResponse: () -> Unit): GrpcRequestedMessageDetails {
        return GrpcRequestedMessageDetails(id, storedMessage, this, onResponse)
    }

    override fun addStreamInfo() {
        val grpcPointers = MessageStreamPointers.newBuilder().addAllMessageStreamPointer(this.streamInfo.toGrpc());
        return channelMessages.addMessage(MessageSearchResponse.newBuilder().setMessageStreamPointers(grpcPointers).build())
    }

}

class GrpcRequestedMessageDetails(
    id: String,
    storedMessage: StoredMessage,
    override val context: GrpcMessageRequestContext,
    onResponse: () -> Unit
) : RequestedMessageDetails(id, storedMessage, context, onResponse) {

    override fun responseMessageInternal() {
        val msg = GrpcMessageProducer.createMessage(this)
        context.channelMessages.addMessage(MessageSearchResponse.newBuilder().setMessage(msg).build())
    }

}
