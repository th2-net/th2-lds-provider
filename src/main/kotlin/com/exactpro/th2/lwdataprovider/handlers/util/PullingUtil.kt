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

import com.exactpro.cradle.BookId
import com.exactpro.cradle.Direction
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.lwdataprovider.ProviderStreamInfo
import com.exactpro.th2.lwdataprovider.db.CradleGroupRequest
import com.exactpro.th2.lwdataprovider.db.CradleMessageExtractor
import mu.KotlinLogging
import java.time.Instant

private val LOGGER = KotlinLogging.logger { }

fun computeNewParametersForGroupRequest(
    groupsByBook: Map<BookId, Set<String>>,
    startTimestamp: Instant,
    lastTimestamp: Instant,
    parameters: CradleGroupRequest,
    allLoaded: MutableSet<String>,
    streamInfo: ProviderStreamInfo,
    extractor: CradleMessageExtractor,
): Map<BookGroup, GroupParametersHolder> = groupsByBook.asSequence().flatMap { (bookId, groups) ->
    groups.asSequence().mapNotNull { group ->
        if (group in allLoaded) {
            LOGGER.trace { "Skip pulling data for group $group because all data is already loaded" }
            return@mapNotNull null
        }
        LOGGER.info { "Pulling updates for group $group" }
        val hasDataOutsideRange = extractor.hasMessagesInGroupAfter(group, bookId, lastTimestamp)
        if (hasDataOutsideRange) {
            LOGGER.info { "Group '$group' in book '$bookId' has data outside the request range" }
            allLoaded += group
        }
        LOGGER.info { "Requesting additional data for group $group" }
        val lastGroupTimestamp = streamInfo.lastTimestampForGroup(group)
            .coerceAtLeast(startTimestamp)
        val newParameters = if (lastGroupTimestamp == startTimestamp) {
            parameters
        } else {
            val lastIdByStream: Map<Pair<String, Direction>, StoredMessageId> = streamInfo.lastIDsForGroup(group)
                .associateBy { it.sessionAlias to it.direction }
            parameters.copy(
                preFilter = { msg ->
                    val stream = msg.run { sessionAlias to direction }
                    lastIdByStream[stream]?.run {
                        msg.sequence > sequence
                    } ?: true
                }
            )
        }
        BookGroup(group, bookId) to GroupParametersHolder(lastGroupTimestamp, newParameters)
    }
}.toMap()

data class GroupParametersHolder(
    val newStartTime: Instant,
    val params: CradleGroupRequest,
)

data class BookGroup(
    val group: String,
    val bookId: BookId,
)

data class BookScope(
    val scope: String,
    val bookId: BookId,
)