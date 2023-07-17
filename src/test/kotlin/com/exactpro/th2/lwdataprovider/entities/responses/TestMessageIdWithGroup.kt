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
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import strikt.assertions.isNull

internal class TestMessageIdWithGroup {
    @Test
    fun `parsed without group`() {
        val stringValue = "book:alias:1:20220101010101000000001:1686312182591833810"
        val id = StoredMessageId.fromString(stringValue)
        expectThat(MessageIdWithGroup.fromString(stringValue)) {
            get { group }.isNull()
            get { messageId } isEqualTo id
        }
    }

    @Test
    fun `parsed with group`() {
        val stringValue = "book:alias:1:20220101010101000000001:1686312182591833810"
        val stringValueWithGroup = "book:group:alias:1:20220101010101000000001:1686312182591833810"
        val id = StoredMessageId.fromString(stringValue)
        expectThat(MessageIdWithGroup.fromString(stringValueWithGroup)) {
            get { group } isEqualTo "group"
            get { messageId } isEqualTo id
        }
    }
}