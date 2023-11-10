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

package com.exactpro.th2.lwdataprovider

import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageMetadata
import com.exactpro.th2.common.grpc.Value
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.lwdataprovider.producers.JsonFormatter
import com.exactpro.th2.lwdataprovider.transport.toProtoDirection
import com.google.gson.Gson
import java.time.Instant

abstract class AbstractJsonFormatter : JsonFormatter {
    private val sb1: StringBuilder = StringBuilder()
    private val gson = Gson()
    override fun print(message: Message): String {
        sb1.setLength(0)
        printM(message, sb1)
        return sb1.toString()
    }

    override fun print(sessionGroup: String, message: ParsedMessage): String {
        sb1.setLength(0)
        printTM(sessionGroup, message, sb1)
        return sb1.toString()
    }

    protected abstract fun printV(value: Value, sb: StringBuilder)

    protected abstract fun printDV(value: Any?, sb: StringBuilder)

    protected fun printM(msg: Message, sb: StringBuilder) {
        sb.append("{")
        if (msg.hasMetadata()) {
            printMetadata(msg.metadata, sb)
            sb.append(',')
        }
        sb.append("\"fields\":")
        printMessage(sb, msg)

        sb.append('}')

    }

    protected fun printM(msg: Map<*, *>, sb: StringBuilder) {
        sb.append("{")
        sb.append("\"fields\":")
        printMessage(sb, msg)
        sb.append('}')
    }

    private fun printTM(sessionGroup: String, msg: ParsedMessage, sb: StringBuilder) {
        sb.append("{")
        printMetadata(sessionGroup, msg, sb)
        sb.append(',')
        sb.append("\"fields\":{")
        if (msg.body.isNotEmpty()) {
            for (entry in msg.body.entries) {
                sb.append('"').append(entry.key).append("\":")
                printDV(entry.value, sb)
                sb.append(',')
            }
            sb.setLength(sb.length - 1)
        }
        sb.append('}').append('}')
    }

    protected fun printMessageContentOnly(msg: Message, sb: StringBuilder) {
        printMessage(sb, msg)
    }

    protected fun printMessageContentOnly(msg: Map<*, *>, sb: StringBuilder) {
        printMessage(sb, msg)
    }

    private fun printMessage(sb: StringBuilder, msg: Map<*, *>) {
        sb.append('{')
        if (msg.isNotEmpty()) {
            for (entry in msg.entries) {
                sb.append('"').append(entry.key).append("\":")
                printDV(entry.value, sb)
                sb.append(',')
            }
            sb.setLength(sb.length - 1)
        }
        sb.append('}')
    }

    private fun printMessage(sb: StringBuilder, msg: Message) {
        sb.append("{")
        val fieldsMap = msg.fieldsMap
        if (fieldsMap.isNotEmpty()) {
            for (entry in fieldsMap.entries) {
                sb.append('"').append(entry.key).append("\":")
                printV(entry.value, sb)
                sb.append(',')
            }
            sb.setLength(sb.length - 1)
        }
        sb.append('}')
    }

    private fun isNeedToEscape(s: String): Boolean {
        // ascii 32 is space, all chars below should be escaped
        return s.chars()
            .anyMatch { it < 32 || it == QUOTE_CHAR || it == BACK_SLASH }
    }

    protected fun convertStringToJson(s: String, builder: StringBuilder) {
        if (isNeedToEscape(s)) {
            gson.toJson(s, builder)
        } else {
            builder.append('"').append(s).append('"')
        }
    }

    private fun printMetadata(msg: MessageMetadata, sb: StringBuilder) {
        sb.append("\"metadata\":{")
        var first = true
        if (msg.hasId()) {
            sb.append("\"id\":{")
            val id = msg.id
            if (id.hasConnectionId()) {
                sb.append("\"connectionId\":{\"sessionAlias\":\"").append(id.connectionId.sessionAlias).append("\"},")
            }
            sb.append("\"direction\":\"").append(id.direction.name).append("\",")
            sb.append("\"sequence\":\"").append(id.sequence).append("\",")
            if (id.hasTimestamp()) {
                sb.append("\"timestamp\":{\"seconds\":\"").append(id.timestamp.seconds).append("\",\"nanos\":\"")
                    .append(id.timestamp.nanos).append("\"},")
            }
            sb.append("\"subsequence\":[")
            if (id.subsequenceCount > 0) {
                id.subsequenceList.forEach { sb.append(it.toString()).append(',') }
                sb.setLength(sb.length - 1)
            }
            sb.append("]}")
            first = false
        }
        if (msg.messageType.isNotEmpty()) {
            if (!first) {
                sb.append(',')
            }
            sb.append("\"messageType\":\"").append(msg.messageType).append("\"")
            first = false
        }
        if (msg.propertiesCount > 0) {
            if (!first) {
                sb.append(',')
            }
            sb.append("\"properties\":")
            gson.toJson(msg.propertiesMap, sb)
            first = false
        }
        if (msg.protocol.isNotEmpty()) {
            if (!first) {
                sb.append(',')
            }
            sb.append("\"protocol\":\"").append(msg.protocol).append("\"")
        }
        sb.append("}")
    }

    private fun printMetadata(sessionGroup: String, msg: ParsedMessage, sb: StringBuilder) {
        sb.append("\"metadata\":{")
        var first = true
        if (msg.id !== MessageId.DEFAULT) {
            sb.append("\"id\":{")
            val id = msg.id
            if (id.sessionAlias.isNotEmpty() || sessionGroup.isNotEmpty()) {
                sb.append("\"connectionId\":{")
                if (sessionGroup.isNotEmpty()) {
                    sb.append("\"sessionGroup\":\"").append(sessionGroup).append("\"")
                    if (id.sessionAlias.isNotEmpty()) {
                        sb.append(",")
                    }
                }
                if (id.sessionAlias.isNotEmpty()) {
                    sb.append("\"sessionAlias\":\"").append(id.sessionAlias).append("\"")
                }
                sb.append("},")
            }
            sb.append("\"direction\":\"").append(id.direction.toProtoDirection().name).append("\",")
            sb.append("\"sequence\":\"").append(id.sequence).append("\",")
            if (id.timestamp !== Instant.EPOCH) {
                sb.append("\"timestamp\":{\"seconds\":\"").append(id.timestamp.epochSecond).append("\",\"nanos\":\"")
                    .append(id.timestamp.nano).append("\"},")
            }
            sb.append("\"subsequence\":[")
            if (id.subsequence.isNotEmpty()) {
                id.subsequence.forEach { sb.append(it.toString()).append(',') }
                sb.setLength(sb.length - 1)
            }
            sb.append("]}")
            first = false
        }
        if (msg.type.isNotEmpty()) {
            if (!first) {
                sb.append(',')
            }
            sb.append("\"messageType\":\"").append(msg.type).append("\"")
            first = false
        }
        if (msg.metadata.isNotEmpty()) {
            if (!first) {
                sb.append(',')
            }
            sb.append("\"properties\":")
            gson.toJson(msg.metadata, sb)
            first = false
        }
        if (msg.protocol.isNotEmpty()) {
            if (!first) {
                sb.append(',')
            }
            sb.append("\"protocol\":\"").append(msg.protocol).append("\"")
        }
        sb.append("}")
    }

    companion object {
        internal const val QUOTE_CHAR = '"'.code
        internal const val BACK_SLASH = '\\'.code
    }
}