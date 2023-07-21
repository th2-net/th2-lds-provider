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

package com.exactpro.th2.lwdataprovider.handlers.util

import com.exactpro.cradle.messages.GroupedMessageFilterBuilder
import com.exactpro.cradle.messages.MessageFilterBuilder
import com.exactpro.th2.lwdataprovider.entities.requests.MessagesGroupRequest
import com.exactpro.th2.lwdataprovider.entities.requests.SearchDirection
import com.exactpro.th2.lwdataprovider.entities.requests.SseMessageSearchRequest

fun MessageFilterBuilder.modifyFilterBuilderTimestamps(request: SseMessageSearchRequest){
    if (request.searchDirection == SearchDirection.next){
        request.startTimestamp?.let { timestampFrom().isGreaterThanOrEqualTo(it) }
        request.endTimestamp?.let { timestampTo().isLessThan(it) }
    } else {
        request.endTimestamp?.let { timestampFrom().isGreaterThan(it) }
        request.startTimestamp?.let { timestampTo().isLessThanOrEqualTo(it) }
    }
}

fun GroupedMessageFilterBuilder.modifyFilterBuilderTimestamps(request: MessagesGroupRequest){
    if (request.searchDirection == SearchDirection.next){
        request.startTimestamp.let { timestampFrom().isGreaterThanOrEqualTo(it) }
        request.endTimestamp.let { timestampTo().isLessThan(it) }
    } else {
        request.endTimestamp.let { timestampFrom().isGreaterThan(it) }
        request.startTimestamp.let { timestampTo().isLessThanOrEqualTo(it) }
    }
}