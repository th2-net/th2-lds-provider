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

package com.exactpro.th2.lwdataprovider.producers

import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.EventStatus
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.dataprovider.lw.grpc.EventResponse
import com.exactpro.th2.lwdataprovider.entities.responses.Event
import com.exactpro.th2.lwdataprovider.grpc.toGrpcMessageId
import com.google.protobuf.ByteString

class GrpcEventProducer {

    companion object {


        fun createEvent(event: Event): EventResponse {

            return EventResponse.newBuilder().apply {
                eventId = event.eventId.toEventId()
                if (event.parentEventId != null) {
                    parentEventId = event.parentEventId.toEventId()
                }
                if (event.batchId != null) {
                    batchId = event.batchId.toEventId()
                }
                isBatched = event.isBatched
                startTimestamp = event.startTimestamp.toTimestamp()
                if (event.endTimestamp != null) {
                    endTimestamp = event.endTimestamp.toTimestamp()
                }
                status = if (event.successful) EventStatus.SUCCESS else EventStatus.FAILED
                eventName = event.eventName
                if (event.eventType != null) {
                    eventType = event.eventType
                }
                body = ByteString.copyFromUtf8(event.body)
                for (msgId in event.attachedMessageIds) {
                    addAttachedMessageId(msgId.toMessageId())
                }
            }.build()
        }

        private fun String.toEventId(): EventID {
            return EventID.newBuilder().setId(this).build()
        }

        private fun String.toMessageId(): MessageID {
            return StoredMessageId.fromString(this).toGrpcMessageId()
        }

    }

}