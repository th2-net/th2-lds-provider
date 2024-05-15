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

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.deser.std.StdDeserializer

internal class ByteSizeDeserializer : StdDeserializer<Int?>(Int::class.java) {
    override fun deserialize(p: JsonParser, ctxt: DeserializationContext): Int? {
        return when (p.currentToken) {
            JsonToken.VALUE_NUMBER_INT, JsonToken.VALUE_NUMBER_FLOAT -> p.intValue
            JsonToken.VALUE_STRING ->
                convertStringToByteSize(p.valueAsString)

            JsonToken.VALUE_NULL -> null
            else -> error("cannot convert ${p.currentToken.name} to an int")
        }
    }

    private fun convertStringToByteSize(byteSize: String): Int {
        return try {
            when {
                byteSize.endsWith(MEGA_BYTES, ignoreCase = true) ->
                    byteSize.substring(0, byteSize.length - MEGA_BYTES.length)
                        .toInt() * 1024 * 1024

                byteSize.endsWith(KILO_BYTES, ignoreCase = true) ->
                    byteSize.substring(0, byteSize.length - KILO_BYTES.length)
                        .toInt() * 1024

                else -> byteSize.toInt()
            }
        } catch (ex: Exception) {
            throw IllegalArgumentException("cannot parse bytes size value from '$byteSize'", ex)
        }
    }

    companion object {
        private const val MEGA_BYTES = "mb"
        private const val KILO_BYTES = "kb"
        private const val serialVersionUID: Long = 9039695677466910433L
    }
}