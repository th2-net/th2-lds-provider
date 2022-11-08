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

package com.exactpro.th2.lwdataprovider

import com.exactpro.th2.common.message.message
import com.exactpro.th2.common.value.nullValue
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

internal class TestCustomJsonFormatter {
    private val formatter = CustomJsonFormatter()

    @Test
    fun `prints null values correctly`() {
        val message = message().apply {
            putFields("nullValue", nullValue())
        }.build()

        val result = formatter.print(message)
        assertEquals(
            // language JSON
            "{\"fields\":{\"nullValue\":{\"nullValue\":\"NULL_VALUE\"}}}",
            result,
            "unexpected result for null value",
        )
    }
}