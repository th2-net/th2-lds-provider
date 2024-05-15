/*******************************************************************************
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.lwdataprovider.entities.requests

import com.exactpro.cradle.BookId
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.lwdataprovider.entities.internal.ProviderEventId
import com.exactpro.th2.lwdataprovider.grpc.toInstant


// FIXME: this request should be reworked to get rid of string representation for event ID
data class GetEventRequest(
    val batchId: String?,
    val eventId: String
) {

    companion object {

        fun fromEventID(eventID: EventID): GetEventRequest {
            return GetEventRequest(
                // No batch ID in grpc
                null,
                StoredTestEventId(BookId(eventID.bookName), eventID.scope, eventID.startTimestamp.toInstant(), eventID.id).toString(),
            )
        }

        fun fromString(evId: String): GetEventRequest {
            if (!evId.contains('/') && !evId.contains('?')) {
                val split = evId.split(ProviderEventId.DIVIDER)
                if (split.size == 2) {
                    return GetEventRequest(split[0], split[1])
                } else if (split.size == 1) {
                    return GetEventRequest(null, split[0])
                }
            }
            throw IllegalArgumentException("Invalid event id: $evId")
        }

    }

}

