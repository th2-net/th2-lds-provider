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

package com.exactpro.th2.lwdataprovider.entities.responses

import com.exactpro.cradle.BookId
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.EventId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.lwdataprovider.entities.internal.Direction
import com.fasterxml.jackson.databind.json.JsonMapper
import io.netty.buffer.Unpooled
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.time.Instant
import java.util.Base64

internal class TestCustomSerializerKt {
    private val mapper = JsonMapper()
    @ParameterizedTest(name = "char `{0}` does not cause problems")
    @ValueSource(chars = ['\"', '\n', '\r', '\\', '\t', '\b', ':'])
    fun `writes valid json`(escapeCharacter: Char) {
        val timestamp = Instant.now()
        val message = ProviderMessage53Transport(
            timestamp = timestamp,
            direction = Direction.OUT,
            sessionId = "ses${escapeCharacter}sion",
            attachedEventIds = setOf(
                "eve${escapeCharacter}nt",
            ),
            bodyBase64 = Base64.getEncoder().encodeToString(byteArrayOf(42, 43)),
            messageId = StoredMessageId(
                BookId("bo${escapeCharacter}ok"),
                "session${escapeCharacter}Alias",
                com.exactpro.cradle.Direction.SECOND,
                timestamp,
                42L,
            ),
            body = listOf(
                TransportMessageContainer(
                    sessionGroup = "session${escapeCharacter}Group",
                    parsedMessage = ParsedMessage(
                        id = MessageId(
                            sessionAlias = "session${escapeCharacter}Alias",
                            direction = com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction.OUTGOING,
                            sequence = 42L,
                            timestamp = timestamp,
                        ),
                        eventId = EventId(
                            id = "eve${escapeCharacter}nt",
                            scope = "scop${escapeCharacter}e",
                            timestamp = timestamp,
                            book = "bo${escapeCharacter}ok",
                        ),
                        type = "Message${escapeCharacter}Type",
                        metadata = mapOf(
                            "ke${escapeCharacter}y" to "val${escapeCharacter}ue",
                        ),
                        protocol = "proto${escapeCharacter}col",
                        rawBody = Unpooled.wrappedBuffer(
                            """{"test":42}""".toByteArray(Charsets.UTF_8)
                        ),
                    )
                ),
            ),
        )

        val jsonBytes = message.toJSONByteArray()

        assertDoesNotThrow { mapper.readTree(jsonBytes) }
    }
}