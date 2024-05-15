/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.lwdataprovider.configuration

import com.exactpro.th2.common.schema.factory.CommonFactory
import com.fasterxml.jackson.module.kotlin.readValue
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import strikt.assertions.isNotNull
import strikt.assertions.isNull

class TestCustomConfigurationClassDeserialization {
    @ParameterizedTest
    @CsvSource(
        "10,10",
        "5KB,5120",
        "5Kb,5120",
        "5kb,5120",
        "5MB,5242880",
        "5Mb,5242880",
        "5mb,5242880",
    )
    fun `bytes size deserialization`(configValue: String, expectedSize: Int) {
        val cfg = CommonFactory.MAPPER.readValue<CustomConfigurationClass>(
            """
            {
                "batchSizeBytes": "$configValue"
            }
            """.trimIndent()
        )
        expectThat(cfg)
            .get { batchSizeBytes }
            .isNotNull()
            .isEqualTo(expectedSize)
    }

    @Test
    fun `null deserialization`() {
        val cfg = CommonFactory.MAPPER.readValue<CustomConfigurationClass>(
            """
            {
                "batchSizeBytes": null
            }
            """.trimIndent()
        )
        expectThat(cfg)
            .get { batchSizeBytes }
            .isNull()
    }
}