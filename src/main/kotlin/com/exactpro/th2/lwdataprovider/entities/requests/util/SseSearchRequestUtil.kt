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

package com.exactpro.th2.lwdataprovider.entities.requests.util

import com.exactpro.cradle.TimeRelation
import com.exactpro.th2.lwdataprovider.entities.exceptions.InvalidRequestException
import com.exactpro.th2.lwdataprovider.entities.requests.SearchDirection
import java.time.Instant

fun getInitEndTimestamp(passedEndTimestamp: Instant?, resultCountLimit: Int?,
                        searchDirection: SearchDirection) : Instant {
    return if (resultCountLimit != null && passedEndTimestamp == null){
        if(searchDirection == SearchDirection.next){
            Instant.MAX
        } else {
            Instant.MIN
        }
    } else {
        passedEndTimestamp ?: throw InvalidRequestException("If limit is not set, passed end timestamp must be not null")
    }
}

fun invalidRequest(text: String): Nothing = throw InvalidRequestException(text)
