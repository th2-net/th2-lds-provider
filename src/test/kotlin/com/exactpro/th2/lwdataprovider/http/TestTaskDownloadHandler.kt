package com.exactpro.th2.lwdataprovider.http

import com.exactpro.cradle.Direction
import com.exactpro.cradle.messages.StoredGroupedMessageBatch
import com.exactpro.cradle.messages.StoredMessageIdUtils
import com.exactpro.th2.lwdataprovider.util.CradleResult
import com.exactpro.th2.lwdataprovider.util.GroupBatch
import com.exactpro.th2.lwdataprovider.util.createCradleStoredMessage
import io.javalin.http.HttpStatus
import io.javalin.testtools.TestConfig
import okhttp3.OkHttpClient
import org.junit.jupiter.api.Test
import org.mockito.kotlin.argThat
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.whenever
import strikt.api.expect
import strikt.api.expectThat
import strikt.assertions.allIndexed
import strikt.assertions.isEqualTo
import strikt.assertions.isNotNull
import strikt.jackson.has
import strikt.jackson.hasSize
import strikt.jackson.isArray
import strikt.jackson.isObject
import strikt.jackson.isTextual
import strikt.jackson.path
import strikt.jackson.textValue
import java.time.Duration
import java.time.Instant

class TestTaskDownloadHandler : AbstractHttpHandlerTest<TaskDownloadHandler>() {
    override fun createHandler(): TaskDownloadHandler {
        return TaskDownloadHandler(
            configuration,
            convExecutor = context.convExecutor,
            sseResponseBuilder,
            keepAliveHandler = context.keepAliveHandler,
            searchMessagesHandler = context.searchMessagesHandler,
            context.requestsDataMeasurement,
        )
    }

    @Test
    fun `creates task`() {
        startTest { _, client ->
            val response = client.post(
                path = "/download",
                json = mapOf(
                    "resource" to "MESSAGES",
                    "bookID" to "test-book",
                    "startTimestamp" to Instant.now().toEpochMilli(),
                    "endTimestamp" to Instant.now().plusSeconds(10).toEpochMilli(),
                    "groups" to setOf("group1", "group2"),
                    "limit" to 42,
                    "streams" to listOf(
                        mapOf(
                            "sessionAlias" to "test",
                            "directions" to setOf("FIRST"),
                        ),
                    ),
                    "searchDirection" to "previous",
                    "responseFormats" to setOf("BASE_64", "JSON_PARSED"),
                )
            )

            expectThat(response) {
                get { code } isEqualTo HttpStatus.CREATED.code
                jsonBody()
                    .isObject()
                    .has("taskID")
                    .path("taskID")
                    .isTextual()
            }
        }
    }

    @Test
    fun `reports incorrect params`() {
        startTest { _, client ->
            val response = client.post(
                path = "/download",
                json = mapOf(
                    "resource" to "MESSAGES",
                    "bookID" to "",
                    "startTimestamp" to Instant.now().toEpochMilli(),
                    "endTimestamp" to Instant.now().plusSeconds(10).toEpochMilli(),
                    "groups" to emptySet<String>(),
                    "limit" to -5,
                    "searchDirection" to "previous",
                    "responseFormats" to setOf("PROTO_PARSED", "JSON_PARSED"),
                )
            )

            expectThat(response) {
                get { code } isEqualTo HttpStatus.BAD_REQUEST.code
                jsonBody()
                    .isObject()
                    .has("bookID")
                    .has("groups")
                    .has("limit")
                    .has("responseFormats")
            }
        }
    }

    @Test
    fun `removes existing task`() {
        startTest { _, client ->
            val response = client.post(
                path = "/download",
                json = mapOf(
                    "resource" to "MESSAGES",
                    "bookID" to "test-book",
                    "startTimestamp" to Instant.now().toEpochMilli(),
                    "endTimestamp" to Instant.now().plusSeconds(10).toEpochMilli(),
                    "groups" to setOf("group1", "group2"),
                )
            )
            val taskID = response.bodyAsJson()["taskID"].asText()
            val deleteResp = client.delete(
                path = "/download/$taskID"
            )

            expectThat(deleteResp) {
                get { code } isEqualTo HttpStatus.NO_CONTENT.code
            }
        }
    }

    @Test
    fun `checks status for existing task`() {
        startTest { _, client ->
            val createResp = client.post(
                path = "/download",
                json = mapOf(
                    "resource" to "MESSAGES",
                    "bookID" to "test-book",
                    "startTimestamp" to Instant.now().toEpochMilli(),
                    "endTimestamp" to Instant.now().plusSeconds(10).toEpochMilli(),
                    "groups" to setOf("group1", "group2"),
                )
            )
            val taskID = createResp.bodyAsJson()["taskID"].asText()
            val deleteResp = client.get(
                path = "/download/$taskID/status"
            )

            expectThat(deleteResp) {
                get { code } isEqualTo HttpStatus.OK.code
                jsonBody()
                    .isObject()
                    .apply {
                        path("taskID").textValue() isEqualTo taskID
                        path("status").textValue() isEqualTo "CREATED"
                        not().has("errors")
                    }
            }
        }
    }

    @Test
    fun `launches the existing task`() {
        val start = Instant.now()
        doReturn(
            CradleResult(
                generateBatch(start, 6)
            )
        ).whenever(storage).getGroupedMessageBatches(argThat {
            groupName == "test-group" && bookId.name == "test-book"
        })

        startTest { _, client ->
            val createResp = client.post(
                path = "/download",
                json = mapOf(
                    "resource" to "MESSAGES",
                    "bookID" to "test-book",
                    "startTimestamp" to start.toEpochMilli(),
                    "endTimestamp" to Instant.now().toEpochMilli(),
                    "groups" to setOf("test-group"),
                    "responseFormats" to setOf("BASE_64"),
                )
            )
            val taskID = createResp.bodyAsJson()["taskID"].asText()
            val response = client.get("/download/$taskID")

            val expectedTimestamp = StoredMessageIdUtils.timestampToString(start)
            val seconds = start.epochSecond
            val nanos = start.nano
            expectThat(response) {
                get { code } isEqualTo HttpStatus.OK.code
                get { body?.bytes()?.toString(Charsets.UTF_8) }
                    .isNotNull()
                    .isEqualTo(
                        """{"timestamp":{"epochSecond":${seconds},"nano":${nanos}},"direction":"IN","sessionId":"test-0","messageType":"","attachedEventIds":[],"body":{},"bodyBase64":"aGVsbG8=","messageId":"test-book:test-0:1:${expectedTimestamp}:1"}
                          |{"timestamp":{"epochSecond":${seconds},"nano":${nanos}},"direction":"IN","sessionId":"test-1","messageType":"","attachedEventIds":[],"body":{},"bodyBase64":"aGVsbG8=","messageId":"test-book:test-1:1:${expectedTimestamp}:2"}
                          |{"timestamp":{"epochSecond":${seconds},"nano":${nanos}},"direction":"IN","sessionId":"test-2","messageType":"","attachedEventIds":[],"body":{},"bodyBase64":"aGVsbG8=","messageId":"test-book:test-2:1:${expectedTimestamp}:3"}
                          |{"timestamp":{"epochSecond":${seconds},"nano":${nanos}},"direction":"IN","sessionId":"test-0","messageType":"","attachedEventIds":[],"body":{},"bodyBase64":"aGVsbG8=","messageId":"test-book:test-0:1:${expectedTimestamp}:4"}
                          |{"timestamp":{"epochSecond":${seconds},"nano":${nanos}},"direction":"IN","sessionId":"test-1","messageType":"","attachedEventIds":[],"body":{},"bodyBase64":"aGVsbG8=","messageId":"test-book:test-1:1:${expectedTimestamp}:5"}
                          |{"timestamp":{"epochSecond":${seconds},"nano":${nanos}},"direction":"IN","sessionId":"test-2","messageType":"","attachedEventIds":[],"body":{},"bodyBase64":"aGVsbG8=","messageId":"test-book:test-2:1:${expectedTimestamp}:6"}
                          |""".trimMargin(marginPrefix = "|")
                    )
            }
        }
    }

    @Test
    fun `report decoding timeout during task execution`() {
        val start = Instant.now()
        doReturn(
            CradleResult(
                generateBatch(start, 3)
            )
        ).whenever(storage).getGroupedMessageBatches(argThat {
            groupName == "test-group" && bookId.name == "test-book"
        })

        startTest { _, client ->
            val createResp = client.post(
                path = "/download",
                json = mapOf(
                    "resource" to "MESSAGES",
                    "bookID" to "test-book",
                    "startTimestamp" to start.toEpochMilli(),
                    "endTimestamp" to Instant.now().toEpochMilli(),
                    "groups" to setOf("test-group"),
                    "responseFormats" to setOf("JSON_PARSED"),
                )
            )
            val taskID = createResp.bodyAsJson()["taskID"].asText()

            val expectedTimestamp = StoredMessageIdUtils.timestampToString(start)
            expect {
                that(client.get("/download/$taskID")) {
                    get { code } isEqualTo HttpStatus.OK.code
                    get { body?.bytes()?.toString(Charsets.UTF_8) }
                        .isNotNull()
                        .isEqualTo(
                            """{"id":"test-book:test-0:1:$expectedTimestamp:1","error":"Codec response wasn\u0027t received during timeout"}
                              |{"id":"test-book:test-1:1:$expectedTimestamp:2","error":"Codec response wasn\u0027t received during timeout"}
                              |{"id":"test-book:test-2:1:$expectedTimestamp:3","error":"Codec response wasn\u0027t received during timeout"}
                              |""".trimMargin(marginPrefix = "|")
                        )
                }
                that(client.get("/download/$taskID/status")) {
                    get { code } isEqualTo HttpStatus.OK.code
                    jsonBody()
                        .isObject() and {
                            path("status").textValue() isEqualTo "COMPLETED_WITH_ERRORS"
                            path("errors").isArray()
                                .hasSize(3)
                                .allIndexed {
                                    path("error").textValue() isEqualTo
                                            "{\"id\":\"test-book:test-$it:1:$expectedTimestamp:${it + 1}\",\"error\":\"Codec response wasn\\u0027t received during timeout\"}"
                                }
                        }
                }
            }
        }
    }

    @Test
    fun `task cannot be started twice`() {
        val start = Instant.now()
        doReturn(
            CradleResult(
                generateBatch(start, 1)
            )
        ).whenever(storage).getGroupedMessageBatches(argThat {
            groupName == "test-group" && bookId.name == "test-book"
        })

        startTest { _, client ->
            val createResp = client.post(
                path = "/download",
                json = mapOf(
                    "resource" to "MESSAGES",
                    "bookID" to "test-book",
                    "startTimestamp" to start.toEpochMilli(),
                    "endTimestamp" to Instant.now().toEpochMilli(),
                    "groups" to setOf("test-group"),
                    "responseFormats" to setOf("BASE_64"),
                )
            )
            val taskID = createResp.bodyAsJson()["taskID"].asText()

            val expectedTimestamp = StoredMessageIdUtils.timestampToString(start)
            val seconds = start.epochSecond
            val nanos = start.nano
            expect {
                that(client.get("/download/$taskID")) {
                    get { code } isEqualTo HttpStatus.OK.code
                    get { body?.bytes()?.toString(Charsets.UTF_8) }
                        .isNotNull()
                        .isEqualTo(
                            """{"timestamp":{"epochSecond":${seconds},"nano":${nanos}},"direction":"IN","sessionId":"test-0","messageType":"","attachedEventIds":[],"body":{},"bodyBase64":"aGVsbG8=","messageId":"test-book:test-0:1:${expectedTimestamp}:1"}
                              |""".trimMargin(marginPrefix = "|")
                        )
                }
                that(client.get("/download/$taskID")) {
                    get { code } isEqualTo HttpStatus.CONFLICT.code
                    jsonBody()
                        .isObject()
                        .path("error")
                        .textValue() isEqualTo "task with id '$taskID' already in progress"
                }
            }
        }
    }

    private fun generateBatch(start: Instant, count: Int, index: Long = 1L): StoredGroupedMessageBatch {
        var startIndex = index
        return GroupBatch(
            "test-group",
            book = "test-book",
            messages = buildList {
                repeat(count) {
                    add(
                        createCradleStoredMessage(
                            "test-${it % 3}",
                            Direction.FIRST,
                            startIndex++,
                            timestamp = start,
                            book = "test-book",
                        )
                    )
                }
            },
        )
    }
}