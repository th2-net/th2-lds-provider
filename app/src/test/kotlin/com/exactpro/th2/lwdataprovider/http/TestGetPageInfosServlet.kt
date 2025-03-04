/*
 * Copyright 2022-2024 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.cradle.PageId
import com.exactpro.cradle.PageInfo
import com.exactpro.cradle.counters.Interval
import com.exactpro.th2.lwdataprovider.util.createPageInfo
import io.javalin.http.HttpStatus
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.eq
import org.mockito.kotlin.whenever
import strikt.api.expectThat
import strikt.assertions.first
import strikt.assertions.isEqualTo
import strikt.assertions.isNotNull
import strikt.jackson.hasSize
import strikt.jackson.isArray
import strikt.jackson.isObject
import strikt.jackson.path
import strikt.jackson.textValue
import java.time.Instant
import java.time.temporal.ChronoUnit

internal class TestGetPageInfosServlet : AbstractHttpHandlerTest<GetPageInfosServlet>() {
    override fun createHandler(): GetPageInfosServlet {
        return GetPageInfosServlet(
            configuration,
            sseResponseBuilder,
            context.keepAliveHandler,
            context.generalCradleHandler,
            context.convExecutor,
            context.requestsDataMeasurement,
        )
    }

    @Test
    fun `returns page infos from cradle`() {
        val start = Instant.parse("2020-10-31T00:00:00Z")
        val end = start.plus(1, ChronoUnit.HOURS)
        // ---|a--|---
        val pageA = createPageInfo("a", start, start.plusSeconds(5))
        // ---|-b-|---
        val pageB = createPageInfo("b", start.plusSeconds(10), end.minusSeconds(10))
        // ---|--c|---
        val pageC = createPageInfo("c", end.minusSeconds(5), end)

        doReturn(listOf(pageA, pageB, pageC).iterator())
            .whenever(storage).getPages(any(), eq(Interval(start, end)))
        startTest { _, client ->
            client.sse(
                "/search/sse/page-infos" +
                        "?startTimestamp=${start.toEpochMilli()}" +
                        "&endTimestamp=${end.toEpochMilli()}" +
                        "&bookId=test"
            ).also { response ->
                expectThat(response.body?.bytes()?.toString(Charsets.UTF_8))
                    .isNotNull()
                    .isEqualTo("""
                        id: 1
                        event: page_info
                        data: {"id":{"book":"test","name":"a"},"comment":"test comment for a","started":{"epochSecond":1604102400,"nano":0},"ended":{"epochSecond":1604102405,"nano":0},"updated":null,"removed":null}
                        
                        id: 2
                        event: page_info
                        data: {"id":{"book":"test","name":"b"},"comment":"test comment for b","started":{"epochSecond":1604102410,"nano":0},"ended":{"epochSecond":1604105990,"nano":0},"updated":null,"removed":null}
                      
                        id: 3
                        event: page_info
                        data: {"id":{"book":"test","name":"c"},"comment":"test comment for c","started":{"epochSecond":1604105995,"nano":0},"ended":{"epochSecond":1604106000,"nano":0},"updated":null,"removed":null}
                    
                        event: close
                        data: empty data
                      
                      
                    """.trimIndent())
            }
        }
    }

    @Test
    fun `returns page infos with minimal filled fields from cradle`() {
        val start = Instant.parse("2020-10-31T00:00:00Z")
        val end = start.plus(1, ChronoUnit.HOURS)
        val pageA = PageInfo(
            /* id = */ PageId(BookId("test-book"), start, "test-page"),
            /* ended = */ null,
            /* comment = */ null,
            /* updated = */ null,
            /* removed = */ null
        )

        doReturn(listOf(pageA).iterator())
            .whenever(storage).getPages(any(), eq(Interval(start, end)))
        startTest { _, client ->
            client.sse(
                "/search/sse/page-infos" +
                        "?startTimestamp=${start.toEpochMilli()}" +
                        "&endTimestamp=${end.toEpochMilli()}" +
                        "&bookId=test"
            ).also { response ->
                expectThat(response.body?.bytes()?.toString(Charsets.UTF_8))
                    .isNotNull()
                    .isEqualTo("""
                        id: 1
                        event: page_info
                        data: {"id":{"book":"test-book","name":"test-page"},"comment":null,"started":{"epochSecond":1604102400,"nano":0},"ended":null,"updated":null,"removed":null}
                        
                        event: close
                        data: empty data
                      
                      
                    """.trimIndent())
            }
        }
    }

    @Test
    fun `returns page infos with limit`() {
        val start = Instant.parse("2020-10-31T00:00:00Z")
        val end = start.plus(1, ChronoUnit.HOURS)
        // ---|a--|---
        val pageA = createPageInfo("a", start, start.plusSeconds(5))
        // ---|-b-|---
        val pageB = createPageInfo("b", start.plusSeconds(10), end.minusSeconds(10))
        // ---|--c|---
        val pageC = createPageInfo("c", end.minusSeconds(5), end)

        doReturn(listOf(pageA, pageB, pageC).iterator())
            .whenever(storage).getPages(any(), eq(Interval(start, end)))
        startTest { _, client ->
            client.sse(
                "/search/sse/page-infos" +
                        "?startTimestamp=${start.toEpochMilli()}" +
                        "&endTimestamp=${end.toEpochMilli()}" +
                        "&resultCountLimit=2" +
                        "&bookId=test"
            ).also { response ->
                expectThat(response.body?.bytes()?.toString(Charsets.UTF_8))
                    .isNotNull()
                    .isEqualTo("""
                        id: 1
                        event: page_info
                        data: {"id":{"book":"test","name":"a"},"comment":"test comment for a","started":{"epochSecond":1604102400,"nano":0},"ended":{"epochSecond":1604102405,"nano":0},"updated":null,"removed":null}
                        
                        id: 2
                        event: page_info
                        data: {"id":{"book":"test","name":"b"},"comment":"test comment for b","started":{"epochSecond":1604102410,"nano":0},"ended":{"epochSecond":1604105990,"nano":0},"updated":null,"removed":null}
                    
                        event: close
                        data: empty data
                      
                      
                    """.trimIndent())
            }
        }
    }

    @Test
    fun `reports error if book is not set`() {
        startTest { _, client ->
            val response = client.sse(
                "/search/sse/page-infos" +
                        "?startTimestamp=${Instant.now().toEpochMilli()}" +
                        "&endTimestamp=${Instant.now().toEpochMilli()}"
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

    @Test
    fun `reports error if startTimestamp is not set`() {
        startTest { _, client ->
            val response = client.sse(
                "/search/sse/page-infos" +
                        "?endTimestamp=${Instant.now().toEpochMilli()}" +
                        "&bookId=test"
            )

            expectThat(response) {
                get { code } isEqualTo HttpStatus.BAD_REQUEST.code
                jsonBody()
                    .isObject()
                    .path("startTimestamp")
                    .isArray()
                    .hasSize(1)
                    .first()
                    .isObject()
                    .path("message").textValue().isEqualTo("NULLCHECK_FAILED")
            }
        }
    }

    @Test
    fun `reports error if endTimestamp is not set`() {
        startTest { _, client ->
            val response = client.sse(
                "/search/sse/page-infos" +
                        "?startTimestamp=${Instant.now().toEpochMilli()}" +
                        "&bookId=test"
            )

            expectThat(response) {
                get { code } isEqualTo HttpStatus.BAD_REQUEST.code
                jsonBody()
                    .isObject()
                    .path("endTimestamp")
                    .isArray()
                    .hasSize(1)
                    .first()
                    .isObject()
                    .path("message").textValue().isEqualTo("NULLCHECK_FAILED")
            }
        }
    }
}