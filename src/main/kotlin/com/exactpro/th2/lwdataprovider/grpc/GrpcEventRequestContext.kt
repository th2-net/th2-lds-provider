/*******************************************************************************
 * Copyright 2022-2022 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.dataprovider.grpc.StreamResponse
import com.exactpro.th2.lwdataprovider.GrpcResponseHandler
import com.exactpro.th2.lwdataprovider.entities.responses.Event
import com.exactpro.th2.lwdataprovider.entities.responses.LastScannedObjectInfo
import com.exactpro.th2.lwdataprovider.http.EventRequestContext
import com.exactpro.th2.lwdataprovider.producers.GrpcEventProducer
import java.util.concurrent.atomic.AtomicLong

class GrpcEventRequestContext (
    override val channelMessages: GrpcResponseHandler,
    requestParameters: Map<String, Any> = emptyMap(),
    counter: AtomicLong = AtomicLong(0L),
    scannedObjectInfo: LastScannedObjectInfo = LastScannedObjectInfo()
) : EventRequestContext(channelMessages, requestParameters, counter, scannedObjectInfo) {

    override fun processEvent(event: Event) {
        val strResp = GrpcEventProducer.createEvent(event)
        channelMessages.addMessage(StreamResponse.newBuilder().setEvent(strResp).build())
        scannedObjectInfo.update(event.eventId, System.currentTimeMillis(), counter)
    }
}