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

package com.exactpro.th2.lwdataprovider.producers

import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.lwdataprovider.CustomProtoJsonFormatter
import com.exactpro.th2.lwdataprovider.CustomSimpleJsonFormatter
import com.exactpro.th2.lwdataprovider.entities.internal.ResponseFormat

object ParsedFormats {
    fun createFormatter(format: ResponseFormat): JsonFormatter {
        return when (format) {
            ResponseFormat.PROTO_PARSED -> CustomProtoJsonFormatter()
            ResponseFormat.JSON_PARSED -> CustomSimpleJsonFormatter()
            ResponseFormat.BASE_64 -> error("not formatter of type $format")
        }
    }
}

interface JsonFormatter {
    fun print(message: Message): String

    fun print(sessionGroup: String, message: ParsedMessage): String
}