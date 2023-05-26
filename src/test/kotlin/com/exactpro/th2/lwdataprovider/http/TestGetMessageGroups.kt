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

import com.exactpro.cradle.BookId
import com.exactpro.cradle.counters.Interval
import com.exactpro.th2.lwdataprovider.util.ImmutableListCradleResult
import io.javalin.http.HttpStatus
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.eq
import org.mockito.kotlin.whenever
import strikt.api.expectThat
import strikt.assertions.containsExactlyInAnyOrder
import strikt.assertions.isEqualTo
import strikt.jackson.isArray
import strikt.jackson.isObject
import strikt.jackson.isTextual
import strikt.jackson.path
import strikt.jackson.textValue
import strikt.jackson.textValues
import java.time.Instant
import java.time.temporal.ChronoUnit

class TestGetMessageGroups : AbstractHttpHandlerTest<GetMessageGroups>() {
    override fun createHandler(): GetMessageGroups = GetMessageGroups(context.searchMessagesHandler)

    @Test
    fun `returns groups`() {
        doReturn(
            setOf("gr1", "gr2")
        ).whenever(storage).getGroups(eq(BookId("test")))

        startTest { _, client ->
            expectThat(client.get("/book/test/message/groups")) {
                get { code } isEqualTo HttpStatus.OK.code
                jsonBody()
                    .isArray()
                    .textValues()
                    .containsExactlyInAnyOrder("gr1", "gr2")
            }
        }
    }

    @Test
    fun `returns groups for time interval`() {
        val start = Instant.now().truncatedTo(ChronoUnit.MILLIS)
        val end = start.plus(1, ChronoUnit.HOURS)
        doReturn(
            ImmutableListCradleResult(setOf("gr1", "gr2"))
        ).whenever(storage).getSessionGroups(eq(BookId("test")), eq(Interval(start, end)))

        startTest { _, client ->
            expectThat(client.get("/book/test/message/groups" +
                    "?startTimestamp=${start.toEpochMilli()}&endTimestamp=${end.toEpochMilli()}")) {
                get { code } isEqualTo HttpStatus.OK.code
                jsonBody()
                    .isArray()
                    .textValues()
                    .containsExactlyInAnyOrder("gr1", "gr2")
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = ["startTimestamp", "endTimestamp"])
    fun `reports error if only one parameter is specified`(paramName: String) {
        startTest { _, client ->
            expectThat(client.get("/book/test/message/groups" +
                    "?$paramName=${Instant.now().toEpochMilli()}")) {
                get { code } isEqualTo HttpStatus.BAD_REQUEST.code
                jsonBody()
                    .isObject()
                    .path("title")
                    .isTextual()
                    .textValue() isEqualTo "InvalidRequestException: either both startTimestamp and endTimestamp must be specified or neither of them"
            }
        }
    }
}