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

package com.exactpro.th2.lwdataprovider.entities.requests.converter

import com.exactpro.th2.lwdataprovider.filter.FilterRequest

class HttpFilterConverter {
    fun convert(params: Map<String, Collection<String>>): Collection<FilterRequest> {
        val filters: Collection<String> = params["filters"] ?: return emptyList()
        return filters.map { name ->
            val singleName = "$name-value"
            val multipleName = "$name-values"
            val values: List<String> = params.entries.asSequence()
                .filter { (key, _) ->
                    key.equals(singleName, ignoreCase = true) || key.equals(multipleName, ignoreCase = true)
                }.flatMap { it.value }.toList()
            val negative: Boolean = params.getBooleanParam(name, "negative")
            val conjunct: Boolean = params.getBooleanParam(name, "conjunct")
            FilterRequest(name, negative, values, conjunct)
        }
    }

    private fun Map<String, Collection<String>>.getBooleanParam(filterName: String, paramName: String): Boolean {
        return entries.asSequence()
            .filter { it.key.equals("$filterName-$paramName", ignoreCase = true) }
            .flatMap { it.value }
            .toList().run {
                when (size) {
                    0 -> false
                    1 -> single().toBoolean()
                    else -> error("only one $filterName-$paramName parameter must be specified")
                }
            }
    }
}