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

package com.exactpro.th2.lwdataprovider.http.serializers

import com.exactpro.cradle.BookId
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.deser.std.StdDeserializer

internal class BookIdDeserializer : StdDeserializer<BookId>(BookId::class.java) {
    override fun deserialize(p: JsonParser, ctxt: DeserializationContext): BookId {
        require(p.currentToken == JsonToken.VALUE_STRING) { "value must be a string" }
        return BookId(p.valueAsString)
    }

    companion object {
        private const val serialVersionUID: Long = -5889633617969813739L
    }
}