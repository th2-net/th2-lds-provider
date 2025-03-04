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

package com.exactpro.th2.lwdataprovider

import com.exactpro.th2.common.grpc.Direction.SECOND
import com.exactpro.th2.common.util.toInstant
import com.exactpro.th2.dataprovider.lw.grpc.DataProviderService
import com.exactpro.th2.dataprovider.lw.grpc.MessageGroupsSearchRequest
import com.exactpro.th2.dataprovider.lw.grpc.MessageSearchResponse
import com.exactpro.th2.lwdataprovider.MessageSearcher.Companion.DEFAULT_SEARCH_STEP
import com.exactpro.th2.lwdataprovider.MessageSearcher.Companion.create
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoMoreInteractions
import org.mockito.kotlin.whenever
import java.time.Duration
import java.time.Instant
import java.util.function.Function
import kotlin.math.ceil
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class MessageSearcherTest {
    private val service: DataProviderService = mock { }
    private val searcher = create(service, SEARCH_STEP)

    @AfterEach
    fun afterEach() {
        verifyNoMoreInteractions(service)
    }

    @Test
    fun `test iterations`() {
        val generator = sequence {
            while (true) {
                yield(MessageSearchResponse.getDefaultInstance())
            }
        }

        val times = 15
        val responses = 10
        val filter = mock<Function<MessageSearchResponse, Boolean>> {
            on { apply(any()) }.thenReturn(false)
        }

        whenever(service.searchMessageGroups(any())).thenAnswer {
            generator.take(responses).iterator()
        }

        val result = searcher.findLastOrNull(
            TEST_BOOK,
            TEST_SESSION_GROUP,
            TEST_SESSION_ALIAS,
            TEST_DIRECTION,
            SEARCH_STEP.multipliedBy(times.toLong()),
            filter
        )

        assertNull(result)

        verify(filter, times(responses * times)).apply(any())
        val captor = argumentCaptor<MessageGroupsSearchRequest> { }
        verify(service, times(times)).searchMessageGroups(captor.capture())

        with(captor.allValues) {
            var previous = Instant.MAX
            forEach { request ->
                val start = request.startTimestamp.toInstant()
                val end = request.endTimestamp.toInstant()
                if (previous == Instant.MAX) {
                    assertTrue("Start time $start isn't after $previous") {
                        previous.isAfter(start)
                    }
                } else {
                    assertEquals(previous, start, "Current start time isn't equal previous end time")
                }
                assertTrue("End time $end isn't after $start") {
                    start.isAfter(end)
                }
                previous = end

                assertEquals(TEST_BOOK, request.bookId.name)
                assertEquals(1, request.messageGroupCount)
                assertEquals(TEST_SESSION_GROUP, request.getMessageGroup(0).name)
                assertEquals(1, request.streamCount)
                assertEquals(TEST_SESSION_ALIAS, request.streamList.single().name)
                assertEquals(TEST_DIRECTION, request.streamList.single().direction)
            }
        }
    }

    @Test
    fun `test withSearchStep function`() {
        val generator = sequence {
            while (true) {
                yield(MessageSearchResponse.getDefaultInstance())
            }
        }

        val filter = mock<Function<MessageSearchResponse, Boolean>> {
            on { apply(any()) }.thenReturn(false)
        }

        val responses = 10
        whenever(service.searchMessageGroups(any())).thenAnswer {
            generator.take(responses).iterator()
        }

        val result = searcher.withSearchStep(SEARCH_STEP)
            .findLastOrNull(
                TEST_BOOK,
                TEST_SESSION_GROUP,
                TEST_SESSION_ALIAS,
                TEST_DIRECTION,
                SEARCH_STEP,
                filter
            )

        assertNull(result)

        verify(filter, times(responses)).apply(any())
        val captor = argumentCaptor<MessageGroupsSearchRequest> { }
        verify(service).searchMessageGroups(captor.capture())

        with(captor.allValues) {
            forEach { request ->
                assertEquals(SEARCH_STEP, Duration.between(request.endTimestamp.toInstant(), request.startTimestamp.toInstant()))
                assertEquals(TEST_BOOK, request.bookId.name)
                assertEquals(1, request.messageGroupCount)
                assertEquals(TEST_SESSION_GROUP, request.getMessageGroup(0).name)
                assertEquals(1, request.streamCount)
                assertEquals(TEST_SESSION_ALIAS, request.streamList.single().name)
                assertEquals(TEST_DIRECTION, request.streamList.single().direction)
            }
        }
    }

    @Test
    fun `test findLastOrNull without session group`() {
        var counter = 0
        val generator = sequence {
            while (true) {
                yield(
                    MessageSearchResponse.newBuilder().apply {
                        messageBuilder.apply {
                            putMessageProperties(PROPERTY, (--counter).toString())
                        }
                    }.build()
                )
            }
        }

        val responses = 10
        whenever(service.searchMessageGroups(any())).thenAnswer {
            generator.take(responses).iterator()
        }

        val target = -5
        val result = searcher.withSearchStep(SEARCH_STEP)
            .findLastOrNull(
                TEST_BOOK,
                TEST_SESSION_ALIAS,
                TEST_DIRECTION,
                SEARCH_STEP,
            ) {
                it.message.getMessagePropertiesOrDefault(PROPERTY, "") == target.toString()
            }

        assertNotNull(result)
        assertEquals(target.toString(), result.message.getMessagePropertiesOrDefault(PROPERTY, ""))

        val captor = argumentCaptor<MessageGroupsSearchRequest> { }
        verify(service).searchMessageGroups(captor.capture())

        with(captor.allValues) {
            forEach { request ->
                assertEquals(SEARCH_STEP, Duration.between(request.endTimestamp.toInstant(), request.startTimestamp.toInstant()))
                assertEquals(TEST_BOOK, request.bookId.name)
                assertEquals(1, request.messageGroupCount)
                assertEquals(TEST_SESSION_ALIAS, request.getMessageGroup(0).name)
                assertEquals(1, request.streamCount)
                assertEquals(TEST_SESSION_ALIAS, request.streamList.single().name)
                assertEquals(TEST_DIRECTION, request.streamList.single().direction)
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = [1, 9, 10, 11])
    fun `test result`(index: Int) {
        var counter = 0
        val generator = sequence {
            while (true) {
                yield(
                    MessageSearchResponse.newBuilder().apply {
                        messageBuilder.apply {
                            putMessageProperties(PROPERTY, (--counter).toString())
                        }
                    }.build()
                )
            }
        }

        val responses = 10
        val target = counter - index
        val times = ceil(index.toDouble() / responses)

        whenever(service.searchMessageGroups(any())).thenAnswer {
            generator.take(responses).iterator()
        }
        val result = searcher.findLastOrNull(
            TEST_BOOK,
            TEST_SESSION_GROUP,
            TEST_SESSION_ALIAS,
            TEST_DIRECTION,
            SEARCH_STEP.multipliedBy(times.toLong())
        ) {
            it.message.getMessagePropertiesOrDefault(PROPERTY, "") == target.toString()
        }

        verify(service, times(times.toInt())).searchMessageGroups(any())
        assertNotNull(result)
        assertEquals(target.toString(), result.message.getMessagePropertiesOrDefault(PROPERTY, ""))
    }

    companion object {
        private const val PROPERTY = "test-property"
        private const val TEST_BOOK = "test-book"
        private const val TEST_SESSION_GROUP = "test-session-group"
        private const val TEST_SESSION_ALIAS = "test-session-alias"
        private val TEST_DIRECTION = SECOND

        private val SEARCH_STEP = DEFAULT_SEARCH_STEP.dividedBy(2)
    }
}