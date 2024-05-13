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

import com.exactpro.th2.dataprovider.lw.grpc.MessageSearchResponse
import com.exactpro.th2.dataprovider.lw.grpc.MessageStreamPointers
import com.exactpro.th2.lwdataprovider.GrpcEvent
import com.exactpro.th2.lwdataprovider.RequestedMessageDetails
import com.exactpro.th2.lwdataprovider.db.DataMeasurement
import com.exactpro.th2.lwdataprovider.entities.exceptions.HandleDataException
import com.exactpro.th2.lwdataprovider.entities.internal.ResponseFormat
import com.exactpro.th2.lwdataprovider.handlers.MessageResponseHandler
import com.exactpro.th2.lwdataprovider.producers.GrpcMessageProducer
import java.util.concurrent.BlockingQueue

class GrpcMessageResponseHandler(
    private val buffer: BlockingQueue<GrpcEvent>,
    dataMeasurement: DataMeasurement,
    maxMessagesPerRequest: Int = 0,
    private val responseFormats: Set<ResponseFormat> = emptySet(),
) : MessageResponseHandler(dataMeasurement, maxMessagesPerRequest) {
    override fun handleNextInternal(data: RequestedMessageDetails) {
        if (!isAlive) return
        buffer.put(GrpcEvent(message = {
            val msg = GrpcMessageProducer.createMessage(data.awaitAndGet(), responseFormats)
            MessageSearchResponse.newBuilder().setMessage(msg).build()
        }))
    }

    override fun complete() {
        if (!isAlive) return
        val grpcPointers = MessageStreamPointers.newBuilder().addAllMessageStreamPointer(streamInfo.toGrpc())
        buffer.put(GrpcEvent(message = { MessageSearchResponse.newBuilder().setMessageStreamPointers(grpcPointers).build() }))
        buffer.put(GrpcEvent(close = true))
    }

    //TODO: use ids
    override fun writeErrorMessage(text: String, id: String?, batchId: String?) {
        writeErrorMessage(HandleDataException(text))
    }

    //TODO: use ids
    override fun writeErrorMessage(error: Throwable, id: String?, batchId: String?) {
        buffer.put(GrpcEvent(error = error))
    }

}
