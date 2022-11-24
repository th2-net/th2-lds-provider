/*
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
 */

package com.exactpro.th2.lwdataprovider.entities.requests

import com.exactpro.cradle.BookId
import java.time.Duration
import java.time.Instant

data class QueueEventsScopeRequest(
    val scopesByBook: Map<BookId, Set<String>>,
    val startTimestamp: Instant,
    val endTimestamp: Instant,
    val syncInterval: Duration,
    val keepAlive: Boolean,
    val externalQueue: String,
) {
    init {
        require(startTimestamp < endTimestamp) { "$startTimestamp must be less than $endTimestamp" }
        require(!syncInterval.isNegative && !syncInterval.isZero) { "$syncInterval must be greater than 0" }
        require(externalQueue.isNotBlank()) { "external queue '$externalQueue' cannot be blank" }
    }
    companion object {
        @JvmStatic
        fun create(
            scopesByBook: Map<BookId, Set<String>>?,
            startTimestamp: Instant?,
            endTimestamp: Instant?,
            syncInterval: Duration?,
            keepAlive: Boolean?,
            externalQueue: String?,
        ): QueueEventsScopeRequest = QueueEventsScopeRequest(
            requireNotNull(scopesByBook) { "scopesByBook must be set" },
            requireNotNull(startTimestamp) { "start timestamp must be set" },
            requireNotNull(endTimestamp) { "end timestamp must be set" },
            requireNotNull(syncInterval) { "sync interval must be set" },
            keepAlive ?: false,
            requireNotNull(externalQueue) { "external queue must be set" },
        )
    }
}