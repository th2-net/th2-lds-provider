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

package com.exactpro.th2.lwdataprovider.http

import com.exactpro.cradle.BookId
import com.exactpro.cradle.Direction
import com.exactpro.th2.common.event.EventUtils
import com.exactpro.th2.lwdataprovider.SseEvent
import com.exactpro.th2.lwdataprovider.SseResponseBuilder
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import com.exactpro.th2.lwdataprovider.db.DataMeasurement
import com.exactpro.th2.lwdataprovider.entities.internal.ResponseFormat
import com.exactpro.th2.lwdataprovider.entities.requests.MessagesGroupRequest
import com.exactpro.th2.lwdataprovider.entities.requests.ProviderMessageStream
import com.exactpro.th2.lwdataprovider.entities.requests.SearchDirection
import com.exactpro.th2.lwdataprovider.entities.responses.ProviderMessage53
import com.exactpro.th2.lwdataprovider.handlers.SearchMessagesHandler
import com.exactpro.th2.lwdataprovider.http.serializers.CustomMillisOrNanosInstantDeserializer
import com.exactpro.th2.lwdataprovider.http.util.JSON_STREAM_CONTENT_TYPE
import com.exactpro.th2.lwdataprovider.http.util.writeJsonStream
import com.exactpro.th2.lwdataprovider.workers.KeepAliveHandler
import com.exactpro.th2.lwdataprovider.workers.TaskID
import com.exactpro.th2.lwdataprovider.workers.TaskInformation
import com.exactpro.th2.lwdataprovider.workers.TaskManager
import com.exactpro.th2.lwdataprovider.workers.TaskStatus
import com.fasterxml.jackson.annotation.JsonFormat
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.fasterxml.jackson.datatype.jsr310.ser.InstantSerializer
import io.javalin.Javalin
import io.javalin.http.Context
import io.javalin.http.HttpStatus
import io.javalin.http.bodyValidator
import io.javalin.openapi.HttpMethod
import io.javalin.openapi.Nullability
import io.javalin.openapi.OpenApi
import io.javalin.openapi.OpenApiContent
import io.javalin.openapi.OpenApiDescription
import io.javalin.openapi.OpenApiExample
import io.javalin.openapi.OpenApiParam
import io.javalin.openapi.OpenApiPropertyType
import io.javalin.openapi.OpenApiRequestBody
import io.javalin.openapi.OpenApiResponse
import mu.KotlinLogging
import java.time.Instant
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.Executor
import java.util.function.Supplier

class TaskDownloadHandler(
    private val configuration: Configuration,
    private val convExecutor: Executor,
    private val sseResponseBuilder: SseResponseBuilder,
    private val keepAliveHandler: KeepAliveHandler,
    private val searchMessagesHandler: SearchMessagesHandler,
    private val dataMeasurement: DataMeasurement,
    private val taskManager: TaskManager,
) : JavalinHandler {

    override fun setup(app: Javalin, context: JavalinContext) {
        app.post(DOWNLOAD_ROUTE, this::registerTask)
        app.get(TASK_STATUS_ROUTE, this::getTaskStatus)
        app.get(TASK_ROUTE, this::executeTask)
        app.delete(TASK_ROUTE, this::deleteTask)
    }

    @OpenApi(
        path = TASK_ROUTE,
        methods = [HttpMethod.DELETE],
        pathParams = [
            OpenApiParam(
                name = TASK_ID,
                description = "task ID",
                required = true,
            ),
        ],
        responses = [
            OpenApiResponse(
                status = "204",
                description = "task successfully removed",
            ),
            OpenApiResponse(
                status = "404",
                content = [OpenApiContent(from = ErrorMessage::class)],
                description = "task with specified ID is not found",
            )
        ]
    )
    private fun deleteTask(context: Context) {
        val taskID = TaskID.create(context.pathParam(TASK_ID))
        LOGGER.info { "Removing task $taskID" }
        val removed = taskManager.remove(taskID)
        if (removed == null) {
            LOGGER.error { "Task $taskID not found" }
            context.status(HttpStatus.NOT_FOUND)
                .json(ErrorMessage("task with id '${taskID.id}' is not found"))
        } else {
            LOGGER.info { "Task $taskID removed" }
            context.status(HttpStatus.NO_CONTENT)
        }
    }

    @OpenApi(
        path = TASK_STATUS_ROUTE,
        methods = [HttpMethod.GET],
        pathParams = [
            OpenApiParam(
                name = TASK_ID,
                description = "task ID",
                required = true,
            ),
        ],
        responses = [
            OpenApiResponse(
                status = "200",
                content = [OpenApiContent(from = TaskStatusResponse::class)],
                description = "task current status",
            ),
            OpenApiResponse(
                status = "404",
                content = [OpenApiContent(from = ErrorMessage::class)],
                description = "task with specified ID is not found",
            ),
        ]
    )
    private fun getTaskStatus(context: Context) {
        val taskID = TaskID.create(context.pathParam(TASK_ID))
        LOGGER.info { "Checking status for task $taskID" }
        val taskInfo = taskManager[taskID] ?: run {
            LOGGER.error { "Task $taskID not found" }
            context.status(HttpStatus.NOT_FOUND)
                .json(ErrorMessage("task with id '${taskID.id}' is not found"))
            return
        }
        context.status(HttpStatus.OK)
            .json(taskInfo.toTaskStatusResponse())
    }

    @OpenApi(
        path = TASK_ROUTE,
        methods = [HttpMethod.GET],
        pathParams = [
            OpenApiParam(
                name = TASK_ID,
                description = "task ID",
                required = true,
            ),
        ],
        responses = [
            OpenApiResponse(
                status = "200",
                content = [
                    OpenApiContent(
                        from = ProviderMessage53::class,
                        mimeType = JSON_STREAM_CONTENT_TYPE,
                    ),
                ],
            ),
            OpenApiResponse(
                status = "404",
                content = [OpenApiContent(from = ErrorMessage::class)],
                description = "task with specified ID is not found",
            ),
            OpenApiResponse(
                status = "409",
                content = [OpenApiContent(from = ErrorMessage::class)],
                description = "task already in progress",
            )
        ]
    )
    private fun executeTask(context: Context) {
        val taskID = TaskID.create(context.pathParam(TASK_ID))
        LOGGER.info { "Executing task $taskID" }
        val taskState: TaskState = taskManager.execute(taskID) {
            if (it == null) {
                return@execute TaskState.NotFound
            }
            val queue = ArrayBlockingQueue<Supplier<SseEvent>>(configuration.responseQueueSize)
            val handler = HttpMessagesRequestHandler(
                queue, sseResponseBuilder, convExecutor, dataMeasurement,
                maxMessagesPerRequest = configuration.bufferPerQuery,
                responseFormats = it.request.responseFormats
                    ?: configuration.responseFormats,
                failFast = it.request.failFast,
            )
            if (!it.attachHandler(handler)) {
                return@execute TaskState.AlreadyInProgress
            }
            TaskState.Ready(it, handler, queue)
        }
        when (taskState) {
            TaskState.AlreadyInProgress -> {
                LOGGER.error { "Task $taskID already in progress" }
                context.status(HttpStatus.CONFLICT)
                    .json(ErrorMessage("task with id '${taskID.id}' already in progress"))
            }

            TaskState.NotFound -> {
                LOGGER.error { "Task $taskID not found" }
                context.status(HttpStatus.NOT_FOUND)
                    .json(ErrorMessage("task with id '${taskID.id}' is not found"))
            }

            is TaskState.Ready -> {
                val (taskInfo, handler, queue) = taskState
                keepAliveHandler.addKeepAliveData(handler).use {
                    searchMessagesHandler.loadMessageGroups(taskInfo.request, handler, dataMeasurement)
                    writeJsonStream(context, queue, handler, dataMeasurement, LOGGER, taskInfo)
                    LOGGER.info { "Task $taskID completed with status ${taskInfo.status}" }
                }
            }
        }
    }

    @OpenApi(
        path = DOWNLOAD_ROUTE,
        methods = [HttpMethod.POST],
        requestBody = OpenApiRequestBody(
            required = true,
            content = [
                OpenApiContent(from = CreateTaskRequest::class)
            ]
        ),
        responses = [
            OpenApiResponse(
                status = "201",
                content = [
                    OpenApiContent(from = TaskIDResponse::class)
                ],
                description = "task successfully created",
            ),
            OpenApiResponse(
                status = "404",
                description = "invalid parameters",
            )
        ]
    )
    private fun registerTask(context: Context) {
        val request = context.bodyValidator<CreateTaskRequest>()
            .check(
                CreateTaskRequest::bookID.name,
                { it.bookID.name.isNotEmpty() },
                "empty value",
            )
            .check(
                CreateTaskRequest::groups.name,
                { it.groups.isNotEmpty() },
                "empty value",
            )
            .check(
                CreateTaskRequest::responseFormats.name,
                { ResponseFormat.isValidCombination(it.responseFormats) },
                "only one ${ResponseFormat.PROTO_PARSED} or ${ResponseFormat.JSON_PARSED} must be used",
            )
            .check(
                CreateTaskRequest::limit.name,
                { it.limit == null || it.limit >= 0 },
                "negative limit",
            )
            .get()

        val taskID = TaskID(EventUtils.generateUUID())

        LOGGER.info { "Registering task $taskID" }
        taskManager[taskID] = TaskInformation(taskID, request.toGroupRequest())
        LOGGER.info { "Task $taskID registered" }

        context.status(HttpStatus.CREATED)
            .json(TaskIDResponse(taskID))
    }

    private sealed class TaskState {
        object AlreadyInProgress : TaskState()
        object NotFound : TaskState()
        data class Ready(
            val info: TaskInformation,
            val handler: HttpMessagesRequestHandler,
            val queue: ArrayBlockingQueue<Supplier<SseEvent>>,
        ) : TaskState()
    }

    private class TaskIDResponse(
        @get:OpenApiPropertyType(definedBy = String::class)
        val taskID: TaskID,
    )

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private class TaskStatusResponse(
        @get:OpenApiPropertyType(definedBy = String::class)
        val taskID: TaskID,
        @get:OpenApiPropertyType(definedBy = String::class)
        @field:JsonSerialize(using = InstantSerializer::class)
        @field:JsonFormat(shape = JsonFormat.Shape.STRING)
        val createdAt: Instant,
        @get:OpenApiPropertyType(definedBy = String::class, nullability = Nullability.NULLABLE)
        @field:JsonSerialize(using = InstantSerializer::class)
        @field:JsonFormat(shape = JsonFormat.Shape.STRING)
        val completedAt: Instant? = null,
        val status: TaskStatus,
        @get:OpenApiPropertyType(definedBy = Array<ErrorMessage>::class, nullability = Nullability.NULLABLE)
        val errors: List<ErrorMessage> = emptyList(),
    )

    private fun TaskInformation.toTaskStatusResponse(): TaskStatusResponse =
        TaskStatusResponse(
            taskID = taskID,
            createdAt = creationTime,
            completedAt = completionTime,
            status = status,
            errors = errors.map { holder ->
                ErrorMessage(
                    error = "${holder.message}${holder.cause?.let { " cause $it" } ?: ""}"
                )
            }
        )

    private fun CreateTaskRequest.toGroupRequest(): MessagesGroupRequest {
        return MessagesGroupRequest(
            groups = groups,
            startTimestamp = startTimestamp,
            endTimestamp = endTimestamp,
            keepOpen = false,
            bookId = bookID,
            responseFormats = responseFormats.ifEmpty { configuration.responseFormats },
            includeStreams = streams.asSequence().flatMap { it.toProviderMessageStreams() }
                .toSet(),
            searchDirection = searchDirection,
            limit = limit,
            failFast = failFast,
        )
    }

    private fun MessageStream.toProviderMessageStreams(): Sequence<ProviderMessageStream> {
        return directions.asSequence().map { ProviderMessageStream(sessionAlias, it) }
    }

    private class CreateTaskRequest(
        val resource: Resource,
        @get:OpenApiPropertyType(definedBy = String::class)
        val bookID: BookId,
        @get:OpenApiPropertyType(definedBy = Long::class)
        @get:OpenApiExample(HttpServer.TIME_EXAMPLE)
        @field:JsonDeserialize(using = CustomMillisOrNanosInstantDeserializer::class)
        val startTimestamp: Instant,
        @get:OpenApiPropertyType(definedBy = Long::class)
        @get:OpenApiExample(HttpServer.TIME_EXAMPLE)
        @field:JsonDeserialize(using = CustomMillisOrNanosInstantDeserializer::class)
        val endTimestamp: Instant,
        val groups: Set<String>,
        @get:OpenApiPropertyType(definedBy = Array<ResponseFormat>::class, nullability = Nullability.NULLABLE)
        val responseFormats: Set<ResponseFormat> = emptySet(),
        val limit: Int? = null,
        @get:OpenApiPropertyType(definedBy = Array<MessageStream>::class, nullability = Nullability.NULLABLE)
        val streams: List<MessageStream> = emptyList(),
        @get:OpenApiPropertyType(definedBy = SearchDirection::class, nullability = Nullability.NULLABLE)
        val searchDirection: SearchDirection = SearchDirection.next,
        @get:OpenApiPropertyType(definedBy = Boolean::class, nullability = Nullability.NULLABLE)
        @get:OpenApiDescription("the request will stop right after the first error reported. Enabled by default")
        val failFast: Boolean = true,
    )

    private class MessageStream(
        val sessionAlias: String,
        @get:OpenApiPropertyType(definedBy = Array<Direction>::class, nullability = Nullability.NULLABLE)
        val directions: Set<Direction> = setOf(Direction.SECOND, Direction.FIRST)
    )

    private enum class Resource {
        MESSAGES,
    }

    private class ErrorMessage(
        val error: String,
    )

    companion object {
        private val LOGGER = KotlinLogging.logger { }
        private const val TASK_ID = "taskID"
        private const val DOWNLOAD_ROUTE = "/download"
        private const val TASK_ROUTE = "$DOWNLOAD_ROUTE/{$TASK_ID}"
        private const val TASK_STATUS_ROUTE = "$DOWNLOAD_ROUTE/{$TASK_ID}/status"
    }
}