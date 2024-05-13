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

internal class TestGetAllPageInfosServlet : AbstractHttpHandlerTest<GetAllPageInfosServlet>() {
    override fun createHandler(): GetAllPageInfosServlet {
        return GetAllPageInfosServlet(
            configuration,
            sseResponseBuilder,
            context.keepAliveHandler,
            context.generalCradleHandler,
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

        doReturn(listOf(pageA, pageB, pageC))
            .whenever(storage).getAllPages(any())
        startTest { _, client ->
            client.sse("/search/sse/page-infos/test/all").also { response ->
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
    fun `returns page infos with limit`() {
        val start = Instant.parse("2020-10-31T00:00:00Z")
        val end = start.plus(1, ChronoUnit.HOURS)
        // ---|a--|---
        val pageA = createPageInfo("a", start, start.plusSeconds(5))
        // ---|-b-|---
        val pageB = createPageInfo("b", start.plusSeconds(10), end.minusSeconds(10))
        // ---|--c|---
        val pageC = createPageInfo("c", end.minusSeconds(5), end)

        doReturn(listOf(pageA, pageB, pageC))
            .whenever(storage).getAllPages(any())
        startTest { _, client ->
            client.sse(
                "/search/sse/page-infos/test/all" +
                        "?resultCountLimit=2"
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
    fun `reports error if limit is negative`() {
        startTest { _, client ->
            val response = client.sse(
                "/search/sse/page-infos/test/all?resultCountLimit=-42"
            )

            expectThat(response) {
                get { code } isEqualTo HttpStatus.BAD_REQUEST.code
                jsonBody()
                    .isObject()
                    .path("resultCountLimit")
                    .isArray()
                    .hasSize(1)
                    .first()
                    .isObject()
                    .path("message").textValue().isEqualTo("must be create than zero")
            }
        }
    }
}