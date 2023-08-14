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

package com.exactpro.th2.lwdataprovider.db.integration

import com.exactpro.cradle.BookToAdd
import com.exactpro.cradle.Direction
import com.exactpro.cradle.messages.GroupedMessageBatchToStore
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.th2.common.annotations.IntegrationTest
import com.exactpro.th2.lwdataprovider.db.MessageDataSink
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.TestFactory
import org.mockito.kotlin.any
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import strikt.assertions.single
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.LockSupport

@IntegrationTest
class TestCradleMessageExtractorIntegration : AbstractCradleIntegrationTest() {
    private val testGroup = "test_group"
    private lateinit var batchToStore: GroupedMessageBatchToStore

    @BeforeAll
    fun setupData() {
        var startTime = Instant.now().minus(1, ChronoUnit.MINUTES)
        val bookInfo = cradleStorage.addBook(BookToAdd("test_book1", startTime))
        cradleStorage.addPage(bookInfo.id, "test_page", startTime, "comment")
        val initIndex = System.currentTimeMillis()
        cradleStorage.entitiesFactory.groupedMessageBatch(testGroup).apply {
            repeat(5) {
                addMessage(
                    MessageToStore(
                        bookId = bookInfo.id,
                        sessionAlias = "test-$it",
                        direction = Direction.FIRST,
                        sequence = initIndex + it,
                        timestamp = startTime,
                    )
                )
                startTime = startTime.plusNanos(5)
            }
        }.also(cradleStorage::storeGroupedMessageBatch)
        // We need to have multiple pages for proper testing
        // But because of the verifications in cradle we cannot create pages in the past
        // So we increase timestamp into the future to create a new page
        val offsetMills: Long = 100
        startTime = Instant.now().plusMillis(offsetMills)
        cradleStorage.addPage(bookInfo.id, "test_page2", startTime, "comment")
        // but we cannot write into the future...
        // so we wait until the current time matches the page start time
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(offsetMills))
        batchToStore = cradleStorage.entitiesFactory.groupedMessageBatch(testGroup).apply {
            repeat(5) {
                addMessage(
                    MessageToStore(
                        bookId = bookInfo.id,
                        sessionAlias = "test-$it",
                        direction = Direction.FIRST,
                        sequence = initIndex + it + 6,
                        timestamp = startTime,
                    )
                )
                startTime = startTime.plusNanos(1)
            }
        }
        cradleStorage.storeGroupedMessageBatch(batchToStore)
    }

    @TestFactory
    fun `finds message by group and id`(): Collection<DynamicTest> {

        return listOf(
            DynamicTest.dynamicTest("first in batch") {
                val sink = mock<MessageDataSink<String, StoredMessage>> { }
                val first = batchToStore.messages.first()
                messageExtractor.getMessage(testGroup, first.id, sink)
                assertReceive(sink, testGroup, first)
            },
            DynamicTest.dynamicTest("last in batch") {
                val sink = mock<MessageDataSink<String, StoredMessage>> { }
                val last = batchToStore.messages.last()
                messageExtractor.getMessage(testGroup, last.id, sink)
                assertReceive(sink, testGroup, last)
            },
            DynamicTest.dynamicTest("middle of batch") {
                val sink = mock<MessageDataSink<String, StoredMessage>> { }
                val middle = batchToStore.messages.asSequence().drop(2).first()
                messageExtractor.getMessage(testGroup, middle.id, sink)
                assertReceive(sink, testGroup, middle)
            },
        )
    }

    @TestFactory
    fun `reports error if no message found`(): Collection<DynamicTest> {
        return listOf(
            DynamicTest.dynamicTest("incorrect timestamp") {
                val sink = mock<MessageDataSink<String, StoredMessage>> { }
                val first = batchToStore.messages.first()
                val msgId = StoredMessageId(first.bookId, first.sessionAlias, first.direction, first.timestamp.minusSeconds(120), first.sequence)
                messageExtractor.getMessage(
                    testGroup,
                    msgId,
                    sink,
                )
                assertNotFound(sink, "Message with id $msgId not found")
            },
            DynamicTest.dynamicTest("incorrect sequence") {
                val sink = mock<MessageDataSink<String, StoredMessage>> { }
                val first = batchToStore.messages.first()
                val msgId = StoredMessageId(first.bookId, first.sessionAlias, first.direction, first.timestamp, first.sequence + 42L)
                messageExtractor.getMessage(
                    testGroup,
                    msgId,
                    sink,
                )
                assertNotFound(sink, "Message with id $msgId not found")
            },
            DynamicTest.dynamicTest("incorrect alias") {
                val sink = mock<MessageDataSink<String, StoredMessage>> { }
                val first = batchToStore.messages.first()
                val msgId = StoredMessageId(first.bookId, first.sessionAlias + "42", first.direction, first.timestamp, first.sequence)
                messageExtractor.getMessage(
                    testGroup,
                    msgId,
                    sink,
                )
                assertNotFound(sink, "Message with id $msgId not found")
            },
            DynamicTest.dynamicTest("incorrect direction") {
                val sink = mock<MessageDataSink<String, StoredMessage>> { }
                val first = batchToStore.messages.first()
                val msgId = StoredMessageId(first.bookId, first.sessionAlias, when(first.direction) {
                    Direction.FIRST -> Direction.SECOND
                    Direction.SECOND -> Direction.FIRST
                    null -> error("null direction")
                }, first.timestamp, first.sequence)
                messageExtractor.getMessage(
                    testGroup,
                    msgId,
                    sink,
                )
                assertNotFound(sink, "Message with id $msgId not found")
            },
        )
    }

    private fun assertReceive(sink: MessageDataSink<String, StoredMessage>, group: String, expectedMessage: StoredMessage) {
        val message = argumentCaptor<StoredMessage>()
        verify(sink).onNext(eq(group), message.capture())
        verify(sink, never()).onError(any<String>(), any(), anyOrNull())
        expectThat(message.allValues)
            .single()
            .get { id }.isEqualTo(expectedMessage.id)
    }

    private fun assertNotFound(sink: MessageDataSink<String, StoredMessage>, expectedErrorMessage: String) {
        val errorMessage = argumentCaptor<String>()
        verify(sink, never()).onNext(any(), any<StoredMessage>())
        verify(sink, never()).onNext(any(), any<List<StoredMessage>>())
        verify(sink).onError(errorMessage.capture(), any(), anyOrNull())
        expectThat(errorMessage.allValues)
            .single()
            .isEqualTo(expectedErrorMessage)
    }
}