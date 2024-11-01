/*
 * Copyright 2024 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.lwdataprovider.http.serializers

import com.exactpro.th2.lwdataprovider.entities.requests.converter.HttpFilterConverter
import com.exactpro.th2.lwdataprovider.filter.FilterRequest
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.node.JsonNodeType

internal class FiltersDeserializer : StdDeserializer<Collection<FilterRequest>>(Collection::class.java) {
    override fun deserialize(p: JsonParser, ctxt: DeserializationContext): Collection<FilterRequest> {
        val node = p.codec.readTree<JsonNode>(p) ?: return emptyList()
        check(node.nodeType == JsonNodeType.OBJECT) {
            "Expected ${JsonNodeType.OBJECT} instead of ${node.nodeType} for filter conversion, value: $node"
        }
        val params: Map<String, Collection<String>> = node.fieldNames().asSequence()
            .map { key ->
                val subNode = node.get(key)
                key to when(subNode.nodeType) {
                    JsonNodeType.STRING, JsonNodeType.BOOLEAN -> setOf(subNode.asText())
                    JsonNodeType.ARRAY -> subNode.asSequence().map { element ->
                        check(element.nodeType == JsonNodeType.STRING) {
                            "Expected ${JsonNodeType.STRING} instead of ${element.nodeType} for '$key' field, value: $element"
                        }
                        element.asText()
                    }.toSet()
                    else -> error(
                        "Expected ${JsonNodeType.STRING} or ${JsonNodeType.ARRAY} instead of ${subNode.nodeType} for '$key' field, value: $subNode"
                    )
                }
            }.toMap()
        return HttpFilterConverter.convert(params)
    }

    companion object {
        private const val serialVersionUID: Long = 2794140580320053221L
    }
}