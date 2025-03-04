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

import com.exactpro.cradle.Direction
import com.exactpro.th2.lwdataprovider.entities.exceptions.InvalidRequestException
import com.exactpro.th2.lwdataprovider.entities.requests.ProviderMessageStream
import java.time.Instant

fun getInitEndTimestamp(passedEndTimestamp: Instant?, resultCountLimit: Int?) : Instant? {
    return if (resultCountLimit == null && passedEndTimestamp == null){
        invalidRequest("either end timestamp or count limit should be set")
    } else {
        passedEndTimestamp
    }
}

fun invalidRequest(text: String): Nothing = throw InvalidRequestException(text)

fun convertToMessageStreams(streams: List<String>): List<ProviderMessageStream> = streams.asSequence().flatMap {
    if (it.contains(':')) {
        val parts = it.split(':')
        when (parts.size) {
            2 -> {
                val (alias, direction) = parts
                sequenceOf(ProviderMessageStream(alias, requireNotNull(Direction.byLabel(direction)) { "incorrect direction $direction" }))
            }
            else -> error("incorrect stream '$it'")
        }
    } else {
        sequenceOf(ProviderMessageStream(it, Direction.SECOND), ProviderMessageStream(it, Direction.FIRST))
    }
}.toList()