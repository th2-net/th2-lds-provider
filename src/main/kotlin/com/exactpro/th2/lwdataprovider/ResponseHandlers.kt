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

import com.exactpro.th2.dataprovider.grpc.EventResponse
import com.exactpro.th2.dataprovider.grpc.MessageSearchResponse
import com.exactpro.th2.lwdataprovider.entities.responses.LastScannedObjectInfo
import com.google.gson.Gson
import java.util.Collections
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicLong

interface ResponseHandler {

    fun finishStream()
    fun keepAliveEvent(obj: LastScannedObjectInfo, counter: AtomicLong)
    fun writeErrorMessage(text: String)
    fun writeErrorMessage(error: Throwable)
}

class SseResponseHandler (val buffer: ArrayBlockingQueue<SseEvent>,
                          val responseBuilder: SseResponseBuilder) : ResponseHandler {

    override fun finishStream() {
        buffer.put(SseEvent(event = EventType.CLOSE))
    }

    override fun keepAliveEvent(obj: LastScannedObjectInfo, counter: AtomicLong) {
        buffer.put(responseBuilder.build(obj, counter))
    }

    override fun writeErrorMessage(text: String) {
        buffer.put(SseEvent(Gson().toJson(Collections.singletonMap("message", text)), EventType.ERROR))
    }

    override fun writeErrorMessage(error: Throwable) {
        this.writeErrorMessage("${error.javaClass.simpleName} : ${error.message}")
    }

}

class GrpcResponseHandler(val buffer: ArrayBlockingQueue<GrpcEvent>) : ResponseHandler {

    @Volatile var streamClosed = false

    override fun finishStream() {
        if (!streamClosed)
            buffer.put(GrpcEvent(close = true))
    }

    override fun keepAliveEvent(obj: LastScannedObjectInfo, counter: AtomicLong) {

    }

    override fun writeErrorMessage(text: String) {
        if (!streamClosed)
            buffer.put(GrpcEvent(error = LwDataProviderException(text)))
    }

    override fun writeErrorMessage(error: Throwable) {
        if (!streamClosed)
            buffer.put(GrpcEvent(error = error))
    }

    fun addMessage(resp: MessageSearchResponse) {
        if (!streamClosed)
            buffer.put(GrpcEvent(message = resp))
    }

    fun addEvent(resp: EventResponse) {
        if (!streamClosed)
            buffer.put(GrpcEvent(event = resp))
    }
}

data class GrpcEvent(
    val message: MessageSearchResponse? = null,
    val event: EventResponse? = null,
    val error: Throwable? = null,
    val close: Boolean = false
)