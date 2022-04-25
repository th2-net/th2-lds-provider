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

import java.time.Instant

class MessagesGroupRequest(
    val groups: Set<String>,
    val startTimestamp: Instant,
    val endTimestamp: Instant,
) {
    init {
        check(startTimestamp <= endTimestamp) { "$START_TIMESTAMP_PARAM must be greater than $END_TIMESTAMP_PARAM" }
    }
    companion object {
        private const val GROUP_PARAM = "group"
        private const val START_TIMESTAMP_PARAM = "startTimestamp"
        private const val END_TIMESTAMP_PARAM = "endTimestamp"

        fun fromParametersMap(map: Map<String, List<String>>): MessagesGroupRequest =
            MessagesGroupRequest(
                map[GROUP_PARAM]?.toSet() ?: error("No $GROUP_PARAM param was set"),
                extractInstant(map, START_TIMESTAMP_PARAM),
                extractInstant(map, END_TIMESTAMP_PARAM),
            )

        private fun extractInstant(map: Map<String, List<String>>, paramName: String): Instant =
            (map[paramName] ?: error("No $paramName param was set"))
                .singleOrNull()?.run { Instant.ofEpochMilli(toLong()) }
                ?: error("Unexpected count of $paramName param")
    }
}