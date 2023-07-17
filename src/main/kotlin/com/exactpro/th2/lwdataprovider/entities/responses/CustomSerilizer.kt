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

import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.utils.EscapeUtils.escape
import com.exactpro.cradle.utils.TimeUtils
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.toByteArray
import com.exactpro.th2.lwdataprovider.entities.responses.ser.numberOfDigits
import java.io.ByteArrayOutputStream
import java.io.OutputStream
import java.time.Instant
import kotlin.math.ceil
import kotlin.math.log10
import kotlin.math.max
import kotlin.text.Charsets.UTF_8

private val COMMA = ",".toByteArray(UTF_8).first().toInt()
private val COLON = ":".toByteArray(UTF_8).first().toInt()
private val ZERO = "0".toByteArray(UTF_8).first().toInt()
private val OPENING_CURLY_BRACE = "{".toByteArray(UTF_8).first().toInt()
private val CLOSING_CURLY_BRACE = "}".toByteArray(UTF_8).first().toInt()
private val OPENING_SQUARE_BRACE = "[".toByteArray(UTF_8).first().toInt()
private val CLOSING_SQUARE_BRACE = "]".toByteArray(UTF_8).first().toInt()
private val DOUBLE_QUOTE = """"""".toByteArray(UTF_8).first().toInt()

private val TIMESTAMP_FILED = """"timestamp"""".toByteArray(UTF_8)
private val EPOCH_SECOND_FILED = """"epochSecond"""".toByteArray(UTF_8)
private val NANO_FILED = """"nano"""".toByteArray(UTF_8)
private val DIRECTION_FILED = """"direction"""".toByteArray(UTF_8)
private val SESSION_ID_FILED = """"sessionId"""".toByteArray(UTF_8)
private val MESSAGE_ID_FILED = """"messageId"""".toByteArray(UTF_8)
private val ATTACHED_EVENT_IDS_FILED = """"attachedEventIds"""".toByteArray(UTF_8)
private val BODY_FILED = """"body"""".toByteArray(UTF_8)
private val BODY_BASE_64_FILED = """"bodyBase64"""".toByteArray(UTF_8)
private val METADATA_FILED = """"metadata"""".toByteArray(UTF_8)
private val SUBSEQUENCE_FILED = """"subsequence"""".toByteArray(UTF_8)
private val MESSAGE_TYPE_FILED = """"messageType"""".toByteArray(UTF_8)
private val PROPERTIES_FILED = """"properties"""".toByteArray(UTF_8)
private val PROTOCOL_FILED = """"protocol"""".toByteArray(UTF_8)
private val FIELDS_FILED = """"fields"""".toByteArray(UTF_8)

fun ProviderMessage53Transport.toJSONByteArray(): ByteArray =
    ByteArrayOutputStream(1_024 * 2).apply { // TODO: init size
        write(OPENING_CURLY_BRACE)
        writeTimestamp(timestamp)
        write(COMMA)
        direction?.let {
            writeField(DIRECTION_FILED, direction.name)
            write(COMMA)
        }
        writeField(SESSION_ID_FILED, sessionId)
        write(COMMA)
        writeAttachedEventIds(attachedEventIds)
        body?.let {
            write(COMMA)
            writeBody(body)
        }
        bodyBase64?.let {
            write(COMMA)
            writeFieldWithoutEscaping(BODY_BASE_64_FILED, bodyBase64)
        }
        write(COMMA)
        writeMessageId(messageId)
        write(CLOSING_CURLY_BRACE)
    }.toByteArray()

private fun OutputStream.writeMessageId(messageId: StoredMessageId) {
    write(MESSAGE_ID_FILED)
    write(COLON)
    write(DOUBLE_QUOTE)
    with(messageId) {
        write(escape(bookId.toString()).toByteArray(UTF_8))
        write(COLON)
        write(escape(sessionAlias).toByteArray(UTF_8))
        write(COLON)
        write(direction.label.toByteArray(UTF_8))
        write(COLON)
        TimeUtils.toLocalTimestamp(timestamp).apply {
            writeNumber(year, 4)
            writeTwoDigits(monthValue)
            writeTwoDigits(dayOfMonth)
            writeTwoDigits(hour)
            writeTwoDigits(minute)
            writeTwoDigits(second)
            writeNumber(nano, 9)
        }
        write(COLON)
        write(sequence.toString().toByteArray(UTF_8))
    }
    write(DOUBLE_QUOTE)
}

private fun OutputStream.writeBody(messages: List<TransportMessageContainer>) {
    write(BODY_FILED)
    write(COLON)
    write(OPENING_SQUARE_BRACE)
    messages.forEachIndexed { index, message ->
        val parsedMessage = message.parsedMessage
        if (!parsedMessage.rawBody.isReadable) {
            error("The ${parsedMessage.id} message can't be serialized because raw data is blank")
        }
        if (index != 0) {
            write(COMMA)
        }
        write(OPENING_CURLY_BRACE)
        writeMetadata(parsedMessage)
        write(COMMA)
        writeFields(parsedMessage)
        write(CLOSING_CURLY_BRACE)
    }
    write(CLOSING_SQUARE_BRACE)
}

private fun OutputStream.writeFields(parsedMessage: ParsedMessage) {
    write(FIELDS_FILED)
    write(COLON)
    write(parsedMessage.rawBody.toByteArray())
}

private fun OutputStream.writeMetadata(message: ParsedMessage) {
    write(METADATA_FILED)
    write(COLON)
    write(OPENING_CURLY_BRACE)

    with(message) {
        if (id.subsequence.isNotEmpty()) {
            writeList(SUBSEQUENCE_FILED, id.subsequence)
            write(COMMA)
        }
        writeField(MESSAGE_TYPE_FILED, type)
        if (metadata.isNotEmpty()) {
            write(COMMA)
            writeMap(PROPERTIES_FILED, metadata)
        }
        if (protocol.isNotBlank()) {
            write(COMMA)
            writeField(PROTOCOL_FILED, protocol)
        }
    }

    write(CLOSING_CURLY_BRACE)
}

private fun OutputStream.writeAttachedEventIds(attachedEventIds: Set<String>) {
    write(ATTACHED_EVENT_IDS_FILED)
    write(COLON)
    write(OPENING_SQUARE_BRACE)
    attachedEventIds.forEachIndexed { index, eventId ->
        if (index != 0) {
            write(COMMA)
        }
        write(DOUBLE_QUOTE)
        write(escape(eventId).toByteArray(UTF_8))
        write(DOUBLE_QUOTE)
    }
    write(CLOSING_SQUARE_BRACE)

}

private fun OutputStream.writeTimestamp(timestamp: Instant) {
    write(TIMESTAMP_FILED)
    write(COLON)
    write(OPENING_CURLY_BRACE)
    writeField(EPOCH_SECOND_FILED, timestamp.epochSecond)
    write(COMMA)
    writeField(NANO_FILED, timestamp.nano)
    write(CLOSING_CURLY_BRACE)
}

private fun OutputStream.writeTwoDigits(value: Int) {
    if (value < 10) {
        write(ZERO)
    }
    write(value.toString().toByteArray(UTF_8))
}

private fun OutputStream.writeNumber(value: Int, size: Int) {
    val digits = numberOfDigits(value)
    if (digits < size) {
        repeat(size - digits) {
            write(ZERO)
        }
    }
    write(value.toString().toByteArray(UTF_8))
}

private fun OutputStream.writeFieldWithoutEscaping(name: ByteArray, value: String) {
    write(name)
    write(COLON)
    write(DOUBLE_QUOTE)
    write(value.toByteArray(UTF_8))
    write(DOUBLE_QUOTE)
}

private fun OutputStream.writeField(name: ByteArray, value: String) = writeFieldWithoutEscaping(name, escape(value))
private fun OutputStream.writeField(name: ByteArray, value: Number) {
    write(name)
    write(COLON)
    write(value.toString().toByteArray(UTF_8))
}

private fun OutputStream.writeField(name: String, value: String) {
    write(DOUBLE_QUOTE)
    write(escape(name).toByteArray(UTF_8))
    write(DOUBLE_QUOTE)
    write(COLON)
    write(DOUBLE_QUOTE)
    write(escape(value).toByteArray(UTF_8))
    write(DOUBLE_QUOTE)
}

private fun OutputStream.writeMap(name: ByteArray, value: Map<String, String>) {
    write(name)
    write(COLON)
    write(OPENING_CURLY_BRACE)
    value.onEachIndexed { index, entry ->
        if (index != 0) {
            write(COMMA)
        }
        writeField(entry.key, entry.value)
    }
    write(CLOSING_CURLY_BRACE)
}

private fun OutputStream.writeList(name: ByteArray, value: Collection<Number>) {
    write(name)
    write(COLON)
    write(value.joinToString(",", "[", "]").toByteArray(UTF_8))
}
