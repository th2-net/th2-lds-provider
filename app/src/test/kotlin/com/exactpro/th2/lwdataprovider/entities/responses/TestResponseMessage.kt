/*
 * Copyright 2023-2024 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.BookId
import com.exactpro.cradle.Direction.FIRST
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.DecodeContext
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.EventId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessageCodec
import com.exactpro.th2.lwdataprovider.entities.internal.Direction.IN
import io.netty.buffer.Unpooled
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.encodeToStream
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.io.ByteArrayOutputStream
import java.time.Instant

private const val TEST_BOOK = "test-book"
private const val TEST_SESSION_ALIAS = "test-session-alias"

private const val TEST_SESSION_ID = "test-session-id"

class TestResponseMessage {

    private val timestamp: Instant = Instant.parse("2023-06-19T13:10:05.123456789Z")

    private val messageWithoutOptionalFields = ProviderMessage53Transport(
        timestamp,
        null,
        TEST_SESSION_ID,
        emptySet(),
        emptyList(),
        null,
        StoredMessageId(BookId(TEST_BOOK), TEST_SESSION_ALIAS, FIRST, timestamp, 1)
    )

    private val fullMessage = ProviderMessage53Transport(
        timestamp,
        IN,
        TEST_SESSION_ID,
        setOf("test-event-id"),
        listOf(
            TransportMessageContainer(
                "test-session-group",
                ParsedMessage.builder().apply {
                    setId(
                        MessageId(
                            TEST_SESSION_ALIAS,
                            Direction.INCOMING,
                            1,
                            timestamp,
                            listOf(1, 2, 3)
                        )
                    )
                    setEventId(
                        EventId(
                            "test-event-id",
                            TEST_BOOK,
                            "test-scope",
                            timestamp
                        )
                    )
                    setType("test-message-type")
                    addMetadataProperty("test-property", "test-value")
                    setProtocol("test-protocol")
                    addField("test-field", "test-value")
                }.build().run {
                    val buf = Unpooled.buffer()
                    ParsedMessageCodec.encode(this, buf)
                    ParsedMessageCodec.decode(DecodeContext.create(), buf)
                }
            )
        ),
        "dGVzdC1yYXctYm9keQ==",
        StoredMessageId(BookId(TEST_BOOK), TEST_SESSION_ALIAS, FIRST, timestamp, 1)
    )

    private val jsonMessageWithoutOptionalFields =
        """{"timestamp":{"epochSecond":1687180205,"nano":123456789},"sessionId":"test-session-id","attachedEventIds":[],"body":[],"messageId":"test-book:test-session-alias:1:20230619131005123456789:1"}"""

    private val jsonFullMessage =
        """{"timestamp":{"epochSecond":1687180205,"nano":123456789},"direction":"IN","sessionId":"test-session-id","attachedEventIds":["test-event-id"],"body":[{"metadata":{"subsequence":[1,2,3],"messageType":"test-message-type","properties":{"test-property":"test-value"},"protocol":"test-protocol"},"fields":{"test-field":"test-value"}}],"bodyBase64":"dGVzdC1yYXctYm9keQ==","messageId":"test-book:test-session-alias:1:20230619131005123456789:1"}"""

    @OptIn(ExperimentalSerializationApi::class)
    private val json = Json {
        explicitNulls = false
    }

    @Test
    fun `json encodeToString message without optional field serialisation`() {
        assertEquals(
            jsonMessageWithoutOptionalFields,
            json.encodeToString(ProviderMessage53Transport.serializer(), messageWithoutOptionalFields)
        )
    }

    @OptIn(ExperimentalSerializationApi::class)
    @Test
    fun `json encodeToStream message without optional field serialisation`() {
        assertEquals(
            jsonMessageWithoutOptionalFields,
            String(ByteArrayOutputStream().apply {
                json.encodeToStream(
                    ProviderMessage53Transport.serializer(),
                    messageWithoutOptionalFields,
                    this
                )
            }.toByteArray())
        )
    }

    @Test
    fun `custom to byte array message without optional field serialisation`() {
        assertEquals(jsonMessageWithoutOptionalFields, String(messageWithoutOptionalFields.toJSONByteArray()))
    }

    @Test
    fun `json encodeToString full message serialisation`() {
        assertEquals(jsonFullMessage, json.encodeToString(ProviderMessage53Transport.serializer(), fullMessage))
    }

    @OptIn(ExperimentalSerializationApi::class)
    @Test
    fun `json encodeToStream full message serialisation`() {
        assertEquals(
            jsonFullMessage,
            String(ByteArrayOutputStream().apply {
                json.encodeToStream(
                    ProviderMessage53Transport.serializer(),
                    fullMessage,
                    this
                )
            }.toByteArray())
        )
    }

    @Test
    fun `custom to byte array full message serialisation`() {
        assertEquals(jsonFullMessage, String(fullMessage.toJSONByteArray()))
    }
}