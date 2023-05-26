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
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestFactory
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.eq
import org.mockito.kotlin.whenever
import strikt.api.expectThat
import strikt.assertions.containsExactlyInAnyOrder
import strikt.assertions.first
import strikt.assertions.isEqualTo
import strikt.assertions.isNotNull
import strikt.jackson.isArray
import strikt.jackson.isObject
import strikt.jackson.isTextual
import strikt.jackson.numberValue
import strikt.jackson.path
import strikt.jackson.textValue
import strikt.jackson.textValues
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.CompletableFuture.supplyAsync

class TestGetMessageGroups : AbstractHttpHandlerTest<GetMessageGroups>() {
    override fun createHandler(): GetMessageGroups = GetMessageGroups(context.searchMessagesHandler)

    @Nested
    inner class Json {
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
                expectThat(
                    client.get(
                        "/book/test/message/groups" +
                                "?startTimestamp=${start.toEpochMilli()}&endTimestamp=${end.toEpochMilli()}"
                    )
                ) {
                    get { code } isEqualTo HttpStatus.OK.code
                    jsonBody()
                        .isArray()
                        .textValues()
                        .containsExactlyInAnyOrder("gr1", "gr2")
                }
            }
        }
    }

    @Nested
    inner class Sse {
        @Test
        fun `returns groups`() {
            doReturn(
                setOf("gr1", "gr2")
            ).whenever(storage).getGroups(eq(BookId("test")))

            startTest { _, client ->
                expectThat(supplyAsync { client.sse("/book/test/message/groups/sse") }.get()) {
                    get { code } isEqualTo HttpStatus.OK.code
                    get { body?.string() }
                        .isNotNull()
                        .isEqualTo(
                            """
                            id: 1
                            event: group
                            data: ["gr1","gr2"]
                            
                            
                        """.trimIndent()
                        )
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
                expectThat(
                    supplyAsync {
                        client.sse(
                            "/book/test/message/groups/sse" +
                                    "?startTimestamp=${start.toEpochMilli()}&endTimestamp=${end.toEpochMilli()}"
                        )
                    }.get()
                ) {
                    get { code } isEqualTo HttpStatus.OK.code
                    get { body?.string() }
                        .isNotNull()
                        .isEqualTo(
                            """
                            id: 1
                            event: group
                            data: ["gr1","gr2"]
                            
                            
                            """.trimIndent()
                        )
                }
            }
        }

        @Test
        fun `uses chunk size from request`() {
            doReturn(
                setOf("gr1", "gr2")
            ).whenever(storage).getGroups(eq(BookId("test")))

            startTest { _, client ->
                expectThat(supplyAsync { client.sse("/book/test/message/groups/sse?chunkedSize=1") }.get()) {
                    get { code } isEqualTo HttpStatus.OK.code
                    get { body?.string() }
                        .isNotNull()
                        .isEqualTo(
                            """
                            id: 1
                            event: group
                            data: ["gr1"]
                            
                            id: 2
                            event: group
                            data: ["gr2"]
                            
                            
                        """.trimIndent()
                        )
                }
            }
        }

        @Test
        fun `reports negative chunk size`() {
            doReturn(
                setOf("gr1", "gr2")
            ).whenever(storage).getGroups(eq(BookId("test")))

            startTest { _, client ->
                expectThat(supplyAsync { client.sse("/book/test/message/groups/sse?chunkedSize=-1") }.get()) {
                    get { code } isEqualTo HttpStatus.BAD_REQUEST.code
                    jsonBody()
                        .isObject()
                        .path("chunkedSize")
                        .isArray()
                        .first()
                        .isObject()
                        .apply {
                            path("message").textValue() isEqualTo "NEGATIVE_CHUNK_SIZE"
                            path("value").numberValue() isEqualTo -1
                        }
                }
            }
        }
    }

    @TestFactory
    fun `reports error if only one parameter is specified`(): List<DynamicTest> {
        fun test(path: String, paramName: String) {
            startTest { _, client ->
                expectThat(
                    client.get(
                        "$path?$paramName=${Instant.now().toEpochMilli()}"
                    )
                ) {
                    get { code } isEqualTo HttpStatus.BAD_REQUEST.code
                    jsonBody()
                        .isObject()
                        .path("title")
                        .isTextual()
                        .textValue() isEqualTo "InvalidRequestException: either both startTimestamp and endTimestamp must be specified or neither of them"
                }
            }
        }

        val parameters = setOf("startTimestamp", "startTimestamp")
        return setOf("/book/test/message/groups", "/book/test/message/groups/sse")
            .flatMap { route ->
                parameters.map {
                    DynamicTest.dynamicTest("Route: $route, param: $it") {
                        test(route, it)
                    }
                }
            }
    }
}