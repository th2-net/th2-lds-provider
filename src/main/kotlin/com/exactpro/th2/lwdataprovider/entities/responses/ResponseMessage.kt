/*
 * Copyright 2022-2023 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.lwdataprovider.transport.toProtoDirection
import kotlinx.serialization.ContextualSerializer
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.Serializer
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.buildClassSerialDescriptor
import kotlinx.serialization.descriptors.element
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.encoding.CompositeEncoder
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.encoding.decodeStructure
import kotlinx.serialization.encoding.encodeStructure
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.JsonTransformingSerializer
import java.time.Instant
import kotlinx.serialization.json.*
import kotlinx.serialization.modules.SerializersModule
import kotlinx.serialization.modules.contextual
import kotlinx.serialization.serializer

/**
 * Marker interface to specify the message what can be sent in response to message request
 */
interface ResponseMessage

fun CompositeEncoder.encodeStringElementIfNotEmpty(descriptor: SerialDescriptor, index: Int, value: String) {
    if (value.isNotEmpty()) {
        encodeStringElement(descriptor, index, value)
    }
}

val SERIALIZERS_MODULE = SerializersModule {
    contextual(FieldSerializer)
}

val SUBSEQUENCE_SERIALIZER = serializer<List<Int>>()
val METADATA_SERIALIZER = serializer<Map<String, String>>()
val MESSAGE_SERIALIZER = SERIALIZERS_MODULE.serializer<Map<String, Any>>()
val COLLECTION_SERIALIZER = SERIALIZERS_MODULE.serializer<List<Any>>()

@Serializable(with = TransportMessageContainerSerializer::class)
class TransportMessageContainer(
    val sessionGroup: String,
    val parsedMessage: ParsedMessage
)

@OptIn(ExperimentalSerializationApi::class)
@Serializer(forClass = Instant::class)
object InstantSerializer : KSerializer<Instant> {
    override val descriptor: SerialDescriptor =
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

@OptIn(ExperimentalSerializationApi::class)
@Serializer(forClass = StoredMessageId::class)
object StoredMessageIdSerializer : KSerializer<StoredMessageId> {
    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("StoredMessageId", PrimitiveKind.STRING)
    override fun serialize(encoder: Encoder, value: StoredMessageId) = encoder.encodeString(value.toString())
    override fun deserialize(decoder: Decoder): StoredMessageId = StoredMessageId.fromString(decoder.decodeString())
}

object FieldSerializer : KSerializer<Any> {
    @OptIn(ExperimentalSerializationApi::class)
    override val descriptor: SerialDescriptor = ContextualSerializer(Any::class, null, emptyArray()).descriptor
    override fun deserialize(decoder: Decoder): Any = error("Unsupported decoding")
    override fun serialize(encoder: Encoder, value: Any) {
        when(value) {
            is List<*> -> encoder.encodeSerializableValue(COLLECTION_SERIALIZER, value as List<Any>)
            is Map<*, *> -> encoder.encodeSerializableValue(MESSAGE_SERIALIZER, value as Map<String, Any>)
            else -> encoder.encodeString(value.toString())
        }
    }
}

object TransportMessageContainerSerializer : KSerializer<TransportMessageContainer> {
    private val connectionIdDescriptor = buildClassSerialDescriptor("ConnectionId") {
        element<String>("sessionGroup", isOptional = true)
        element<String>("sessionAlias")
    }
    private val timestampDescriptor = buildClassSerialDescriptor("Timestamp") {
        element<Long>("seconds")
        element<Int>("nanos")
    }
    private val idDescriptor = buildClassSerialDescriptor("Id") {
        element("connectionId", connectionIdDescriptor)
        element<String>("direction")
        element<Long>("sequence")
        element("timestamp", timestampDescriptor)
        element<List<Long>>("subsequence", isOptional = true)
    }
    private val metadataDescriptor = buildClassSerialDescriptor("Metadata") {
        element("id", idDescriptor)
        element<String>("messageType")
        element<Map<String, String>>("properties", isOptional = true)
        element<String>("protocol")
    }

    override val descriptor: SerialDescriptor = buildClassSerialDescriptor("TransportMessage") {
        element("metadata", metadataDescriptor)
        element("fields", MESSAGE_SERIALIZER.descriptor)
    }

    override fun serialize(encoder: Encoder, value: TransportMessageContainer) {
        encoder.encodeStructure(descriptor) {
            with(value.parsedMessage) {
                encodeInlineElement(descriptor, 0).encodeStructure(metadataDescriptor) {
                    encodeInlineElement(metadataDescriptor, 0).encodeStructure(idDescriptor) {
                        with(id) {
                            encodeInlineElement(idDescriptor, 0).encodeStructure(connectionIdDescriptor) {
                                encodeStringElementIfNotEmpty(connectionIdDescriptor, 0, value.sessionGroup)
                                encodeStringElementIfNotEmpty(connectionIdDescriptor, 1, sessionAlias)
                            }
                            encodeStringElementIfNotEmpty(idDescriptor, 1, direction.toProtoDirection().name)
                            encodeLongElement(idDescriptor, 2, sequence)
                            encodeInlineElement(idDescriptor, 3).encodeStructure(timestampDescriptor) {
                                with(timestamp) {
                                    encodeLongElement(timestampDescriptor, 0, epochSecond)
                                    encodeIntElement(timestampDescriptor, 1, nano)
                                }
                            }
                            encodeSerializableElement(idDescriptor, 4, SUBSEQUENCE_SERIALIZER, subsequence)
                        }
                    }
                    encodeStringElementIfNotEmpty(metadataDescriptor, 1, type)
                    if (metadata.isNotEmpty()) encodeSerializableElement(metadataDescriptor, 2, METADATA_SERIALIZER, metadata)
                    encodeStringElementIfNotEmpty(metadataDescriptor, 3, protocol)
                }
                if (body.isNotEmpty()) encodeSerializableElement(descriptor, 1, MESSAGE_SERIALIZER, body)
            }
        }
    }

    override fun deserialize(decoder: Decoder): TransportMessageContainer = error("Unsupported transport message container decoding")
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
}