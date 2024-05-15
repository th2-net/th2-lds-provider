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
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

internal class TestHttpFilterConverter {
    private val converter = HttpFilterConverter()
    @Test
    fun `converts request`() {
        val filterRequests: Collection<FilterRequest> = converter.convert(
            mapOf(
                "filters" to listOf("type", "name"),
                "type-values" to listOf("a", "b", "c"),
                "type-conjunct" to listOf("true"),
                "name-value" to listOf("d"),
                "name-negative" to listOf("true")
            )
        )
        assertEquals(2, filterRequests.size) { "unexpected requests $filterRequests" }
        assertFilter(
            filterRequests, FilterRequest(
                name = "type",
                values = listOf("a", "b", "c"),
                negative = false,
                conjunct = true,
            )
        )
        assertFilter(
            filterRequests, FilterRequest(
                name = "name",
                values = listOf("d"),
                negative = true,
                conjunct = false
            )
        )
    }

    private fun assertFilter(filterRequests: Collection<FilterRequest>, expectedFilter: FilterRequest) {
        val typeFilter = filterRequests.filter { it.name == expectedFilter.name }.also {
            assertEquals(1, it.size) { "unexpected type filters: $it" }
        }.single()
        assertEquals(expectedFilter, typeFilter)
    }
}