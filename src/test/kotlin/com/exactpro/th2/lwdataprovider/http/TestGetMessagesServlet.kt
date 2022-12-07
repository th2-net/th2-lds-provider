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

import io.javalin.http.HttpStatus
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.first
import strikt.assertions.isEqualTo
import strikt.jackson.hasSize
import strikt.jackson.isArray
import strikt.jackson.isObject
import strikt.jackson.path
import strikt.jackson.textValue
import java.time.Instant

internal class TestGetMessagesServlet : AbstractHttpHandlerTest<GetMessagesServlet>() {
    override fun createHandler(): GetMessagesServlet {
        return GetMessagesServlet(
            configuration,
            sseResponseBuilder,
            keepAliveHandler,
            messageHandler,
            dataMeasurement,
        )
    }

    @Test
    fun `reports error if book is not set`() {
        startTest { _, client ->
            val response = client.sse(
                "/search/sse/messages?" +
                        "startTimestamp=${Instant.now().toEpochMilli()}&endTimestamp=${Instant.now().toEpochMilli()}" +
                        "&stream=test"
            )

            expectThat(response) {
                get { code } isEqualTo HttpStatus.BAD_REQUEST.code
                jsonBody()
                    .isObject()
                    .path("bookId")
                    .isArray()
                    .hasSize(1)
                    .first()
                    .isObject()
                    .path("message").textValue().isEqualTo("NULLCHECK_FAILED")
            }
        }
    }
}