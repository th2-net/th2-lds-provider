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

import com.exactpro.th2.dataprovider.grpc.MessageGroupsSearchRequest
import com.exactpro.th2.lwdataprovider.grpc.toInstant
import java.time.Instant

data class MessagesGroupRequest(
    val groups: Set<String>,
    val startTimestamp: Instant,
    val endTimestamp: Instant,
    val sort: Boolean,
    val rawOnly: Boolean,
    val keepOpen: Boolean,
) {
    init {
        check(startTimestamp <= endTimestamp) { "$START_TIMESTAMP_PARAM must be greater than $END_TIMESTAMP_PARAM" }
    }
    companion object {
        private const val GROUP_PARAM = "group"
        private const val START_TIMESTAMP_PARAM = "startTimestamp"
        private const val END_TIMESTAMP_PARAM = "endTimestamp"
        private const val SORT_PARAMETER = "sort"
        private const val RAW_ONLY_PARAMETER = "onlyRaw"
        private const val KEEP_OPEN_PARAMETER = "keepOpen"

        @JvmStatic
        fun fromParametersMap(map: Map<String, List<String>>): MessagesGroupRequest =
            MessagesGroupRequest(
                map[GROUP_PARAM]?.toSet() ?: error("No $GROUP_PARAM param was set"),
                extractInstant(map, START_TIMESTAMP_PARAM),
                extractInstant(map, END_TIMESTAMP_PARAM),
                map.booleanOrDefault(SORT_PARAMETER, false),
                map.booleanOrDefault(RAW_ONLY_PARAMETER, false),
                map.booleanOrDefault(KEEP_OPEN_PARAMETER, false),
            )

        @JvmStatic
        fun fromGrpcRequest(request: MessageGroupsSearchRequest): MessagesGroupRequest =
            MessagesGroupRequest(
                request.messageGroupList.mapTo(HashSet(request.messageGroupCount)) {
                    it.name.apply { check(isNotEmpty()) { "group name cannot be empty" } }
                },
                if (request.hasStartTimestamp()) request.startTimestamp.toInstant() else error("missing start timestamp"),
                if (request.hasEndTimestamp()) request.endTimestamp.toInstant() else error("missing end timestamp"),
                if (request.hasSort()) request.sort.value else false,
                request.rawOnly,
                request.keepOpen,
            )

        private fun Map<String, List<String>>.booleanOrDefault(name: String, default: Boolean): Boolean {
            val params = this[name] ?: return default
            return params.singleOrNull()
                ?.toBoolean() ?: error("More than one parameter $name was specified")
        }

        private fun extractInstant(map: Map<String, List<String>>, paramName: String): Instant =
            (map[paramName] ?: error("No $paramName param was set"))
                .singleOrNull()?.run { Instant.ofEpochMilli(toLong()) }
                ?: error("Unexpected count of $paramName param")
    }
}