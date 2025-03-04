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
import org.apache.commons.lang3.StringUtils.isNotBlank
import java.time.Duration
import java.time.Instant

data class QueueMessageGroupsRequest(
    val groupsByBook: Map<BookId, Set<String>>,
    val startTimestamp: Instant,
    val endTimestamp: Instant,
    val syncInterval: Duration,
    val keepAlive: Boolean,
    val externalQueue: String?,
    val sendRawDirectly: Boolean,
    val rawOnly: Boolean,
) {
    init {
        require(startTimestamp < endTimestamp) {
            "$startTimestamp start timestamp must be less than $endTimestamp end timestamp"
        }
        require(!syncInterval.isNegative && !syncInterval.isZero) {
            "$syncInterval sync interval must be greater than 0"
        }
        require(externalQueue == null || externalQueue.isNotBlank()) {
            "'$externalQueue' external queue must be null or not blank"
        }
        require(!sendRawDirectly || isNotBlank(externalQueue)) {
            "'$externalQueue' external queue must not be blank when $sendRawDirectly send raw directly"
        }
        if (rawOnly) {
            require(sendRawDirectly) { "rawOnly parameter can be used only with sendRawDirectly enabled" }
        }
    }
    companion object {
        @JvmStatic
        fun create(
            groupsByBook: Map<BookId, Set<String>>?,
            startTimestamp: Instant?,
            endTimestamp: Instant?,
            syncInterval: Duration?,
            keepAlive: Boolean?,
            externalQueue: String?,
            sendRawDirectly: Boolean?,
            rawOnly: Boolean?,
        ): QueueMessageGroupsRequest = QueueMessageGroupsRequest(
            requireNotNull(groupsByBook) { "groupsByBook must be set" },
            requireNotNull(startTimestamp) { "start timestamp must be set" },
            requireNotNull(endTimestamp) { "end timestamp must be set" },
            requireNotNull(syncInterval) { "sync interval must be set" },
            keepAlive ?: false,
            externalQueue,
            sendRawDirectly ?: false,
            rawOnly ?: false,
        )
    }
}