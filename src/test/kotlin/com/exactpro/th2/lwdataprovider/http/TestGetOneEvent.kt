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

package com.exactpro.th2.lwdataprovider.http

import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import strikt.assertions.isNotNull

internal class TestGetOneEvent : AbstractHttpHandlerTest<GetOneEvent>() {
    override fun createHandler(): GetOneEvent {
        return GetOneEvent(
            configuration,
            sseResponseBuilder,
            context.searchEventsHandler,
        )
    }

    @Test
    fun `incorrect id`() {
        startTest { _, client ->
            client.get(
                "/event/book:scope:20201031010203123456789:id"
            ).also { response ->
                expectThat(response.body?.bytes()?.toString(Charsets.UTF_8))
                    .isNotNull()
                    .isEqualTo("{\"id\":\"book:scope:20201031010203123456789:id\",\"error\":\"Event is not found with id: \\u0027book:scope:20201031010203123456789:id\\u0027\"}")
            }
        }
    }

    @Test
    fun `invalid id format`() {
        startTest { _, client ->
            client.get(
                "/event/test"
            ).also { response ->
                expectThat(response.body?.bytes()?.toString(Charsets.UTF_8))
                    .isNotNull()
                    .isEqualTo("{\"id\":\"test\",\"error\":\"Test Event ID (test) should contain book ID, scope, timestamp and unique ID delimited with \\u0027:\\u0027\"}")
            }
        }
    }
}