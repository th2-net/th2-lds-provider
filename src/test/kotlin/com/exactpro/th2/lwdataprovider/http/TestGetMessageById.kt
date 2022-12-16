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

import com.exactpro.cradle.Direction
import com.exactpro.th2.lwdataprovider.util.createCradleStoredMessage
import org.junit.jupiter.api.Test
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.eq
import org.mockito.kotlin.whenever
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import strikt.assertions.isNotNull
import java.time.Instant

internal class TestGetMessageById : AbstractHttpHandlerTest<GetMessageById>() {
    override fun createHandler(): GetMessageById {
        return GetMessageById(
            configuration,
            sseResponseBuilder,
            messageHandler,
            dataMeasurement
        )
    }

    @Test
    fun `incorrect message id`() {
        startTest { _, client ->
            client.sse(
                "/message/test:sessionAlias:2:20201031010203123456789:1"
            ).also { response ->
                expectThat(response.body?.bytes()?.toString(Charsets.UTF_8))
                    .isNotNull()
                    .isEqualTo("{\"id\":\"test:sessionAlias:2:20201031010203123456789:1\",\"error\":\"Message with id test:sessionAlias:2:20201031010203123456789:1 not found\"}")
            }
        }
    }

    @Test
    fun `returns message by id`() {
        val timestamp = Instant.parse("2020-10-31T01:02:03.123456789Z")
        val message = createCradleStoredMessage(
            streamName = "test",
            direction = Direction.SECOND,
            index = 1,
            content = "test content",
            timestamp = timestamp,
        )
        doReturn(message)
            .whenever(storage).getMessage(eq(message.id))
        startTest { _, client ->
            client.sse(
                "/message/test:test:2:20201031010203123456789:1"
            ).also { response ->
                expectThat(response.body?.bytes()?.toString(Charsets.UTF_8))
                    .isNotNull()
                    .isEqualTo("{\"id\":\"test:test:2:20201031010203123456789:1\",\"error\":\"Operation hasn\\u0027t done during timeout ${configuration.decodingTimeout} MILLISECONDS\"}")
            }
        }
    }
}