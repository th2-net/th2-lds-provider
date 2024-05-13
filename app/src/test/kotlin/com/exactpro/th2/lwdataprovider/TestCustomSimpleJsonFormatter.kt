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

import com.exactpro.th2.common.message.addField
import com.exactpro.th2.common.message.addFields
import com.exactpro.th2.common.message.message
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

internal class TestCustomSimpleJsonFormatter {
    private val formatter = CustomSimpleJsonFormatter()
    private val objectMapper = ObjectMapper()

    @ParameterizedTest
    @ValueSource(strings = ["\\", "/", "\"", "'", "normal"])
    fun `prints simple field`(value: String) {
        val message = message().addField("a", value).build()
        val result = formatter.print(message)
        val node: JsonNode = assertDoesNotThrow({ "cannot deserialize result: $result" }) {
            objectMapper.readTree(result)
        }
        val fields = node.get("fields")
        assertNotNull(fields) { "no fields in $node" }
        val simpleValue = fields["a"]
        assertNotNull(simpleValue) { "no field 'a' in $node" }
        assertEquals(value, simpleValue.asText(), "incorrect simple value")
    }

    @Test
    fun `prints null field`() {
        val message = message().addField("a", null).build()
        val result = formatter.print(message)
        assertDoesNotThrow { objectMapper.readTree(result) }
        assertEquals("""{"fields":{"a":null}}""", result)
    }

    @Test
    fun `prints collection field`() {
        val message = message().addField("a", listOf(1,2,3)).build()
        val result = formatter.print(message)
        assertDoesNotThrow { objectMapper.readTree(result) }
        assertEquals("""{"fields":{"a":["1","2","3"]}}""", result)
    }

    @Test
    fun `prints empty collection field`() {
        val message = message().addField("a", listOf<String>()).build()
        val result = formatter.print(message)
        assertDoesNotThrow { objectMapper.readTree(result) }
        assertEquals("""{"fields":{"a":[]}}""", result)
    }

    @Test
    fun `prints message field`() {
        val message = message().addField("a", message().addField("b", "1")).build()
        val result = formatter.print(message)
        assertDoesNotThrow { objectMapper.readTree(result) }
        assertEquals("""{"fields":{"a":{"b":"1"}}}""", result)
    }
}