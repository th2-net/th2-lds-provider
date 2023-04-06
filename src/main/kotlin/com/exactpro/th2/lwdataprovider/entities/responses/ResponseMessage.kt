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

package com.exactpro.th2.lwdataprovider.entities.responses

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializer
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.buildClassSerialDescriptor
import kotlinx.serialization.descriptors.element
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.encoding.decodeStructure
import kotlinx.serialization.encoding.encodeStructure
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.JsonTransformingSerializer
import java.time.Instant
import kotlinx.serialization.json.*

/**
 * Marker interface to specify the message what can be sent in response to message request
 */
interface ResponseMessage

@OptIn(ExperimentalSerializationApi::class)
@Serializer(forClass = Instant::class)
object InstantSerializer : KSerializer<Instant> {
    override val descriptor: SerialDescriptor =
        //{"epochSecond":${messageTimestamp.epochSecond},"nano":${messageTimestamp.nano}}
        buildClassSerialDescriptor("Instant") {
            element<Long>("epochSecond")
            element<Int>("nano")
        }
    override fun serialize(encoder: Encoder, value: Instant) = encoder.encodeStructure(descriptor) {
        encodeLongElement(descriptor, 0, value.epochSecond)
        encodeIntElement(descriptor, 1, value.nano)
    }
    override fun deserialize(decoder: Decoder): Instant = decoder.decodeStructure(descriptor) {
        var epochSecond = -1L
        var nano = -1
        while (true) {
            when (val index = decodeElementIndex(descriptor)) {
                0 -> epochSecond = decodeLongElement(descriptor, 0)
                1 -> nano = decodeIntElement(descriptor, 1)
                CompositeDecoder.DECODE_DONE -> break
                else -> error("Unexpected index: $index")
            }
        }
        require(epochSecond >= 0 && nano >= 0)
        Instant.ofEpochSecond(epochSecond, nano.toLong())
    }
}

object UnwrappingJsonListSerializer :
    JsonTransformingSerializer<String>(String.serializer()) {

    @OptIn(ExperimentalSerializationApi::class)
    override fun transformSerialize(element: JsonElement): JsonElement {
        if (element is JsonPrimitive) {
            return JsonUnquotedLiteral(element.content)
        }
        return element
    }
    override fun transformDeserialize(element: JsonElement): JsonElement {
        return super.transformDeserialize(element)
    }
}