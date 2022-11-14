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

import com.exactpro.th2.common.grpc.EventID


data class GetEventRequest(
    val batchId: String?,
    val eventId: String
) {

    companion object {

        fun fromEventID(eventID: EventID) : GetEventRequest {
            val id = eventID.id
            return if (id.contains(':')) {
                val spl = id.split(':')
                GetEventRequest(spl[0], spl[1])
            } else {
                GetEventRequest(null, id)
            }
        }

    }

}

