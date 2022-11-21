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

package com.exactpro.th2.lwdataprovider.handlers.util

import com.exactpro.cradle.Direction
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.lwdataprovider.ProviderStreamInfo
import com.exactpro.th2.lwdataprovider.db.CradleGroupRequest
import com.exactpro.th2.lwdataprovider.db.CradleMessageExtractor
import mu.KotlinLogging
import java.time.Instant

private val LOGGER = KotlinLogging.logger { }

fun computeNewParametersForGroupRequest(
    groups: Set<String>,
    startTimestamp: Instant,
    lastTimestamp: Instant,
    parameters: CradleGroupRequest,
    allLoaded: MutableSet<String>,
    streamInfo: ProviderStreamInfo,
    extractor: CradleMessageExtractor,
): Map<String, CradleGroupRequest> = groups.asSequence().mapNotNull { group ->
    if (group in allLoaded) {
        LOGGER.trace { "Skip pulling data for group $group because all data is already loaded" }
        return@mapNotNull null
    }
    LOGGER.info { "Pulling updates for group $group" }
    val hasDataOutsideRange = extractor.hasMessagesInGroupAfter(group, lastTimestamp)
    if (hasDataOutsideRange) {
        LOGGER.info { "All data in requested range is loaded for group $group" }
        allLoaded += group
    }
    LOGGER.info { "Requesting additional data for group $group" }
    val lastGroupTimestamp = streamInfo.lastTimestampForGroup(group)
        .coerceAtLeast(startTimestamp)
    val newParameters = if (lastGroupTimestamp == startTimestamp) {
        parameters
    } else {
        val lastIdByStream: Map<Pair<String, Direction>, StoredMessageId> = streamInfo.lastIDsForGroup(group)
            .associateBy { it.streamName to it.direction }
        parameters.copy(
            start = lastGroupTimestamp,
            preFilter = { msg ->
                val stream = msg.run { streamName to direction }
                lastIdByStream[stream]?.run {
                    msg.index > index
                } ?: true
            }
        )
    }
    group to newParameters
}.toMap()