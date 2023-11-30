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
import com.exactpro.th2.lwdataprovider.CancelableResponseHandler
import com.exactpro.th2.lwdataprovider.SseEvent
import com.exactpro.th2.lwdataprovider.SseResponseBuilder
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import com.exactpro.th2.lwdataprovider.db.DataMeasurement
import com.exactpro.th2.lwdataprovider.entities.internal.ResponseFormat
import com.exactpro.th2.lwdataprovider.entities.requests.MessagesGroupRequest
import com.exactpro.th2.lwdataprovider.entities.requests.ProviderMessageStream
import com.exactpro.th2.lwdataprovider.entities.requests.SearchDirection
import com.exactpro.th2.lwdataprovider.handlers.SearchMessagesHandler
import com.exactpro.th2.lwdataprovider.http.listener.ProgressListener
import com.exactpro.th2.lwdataprovider.http.serializers.CustomMillisOrNanosInstantDeserializer
import com.exactpro.th2.lwdataprovider.http.util.writeJsonStream
import com.exactpro.th2.lwdataprovider.workers.KeepAliveHandler
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonValue
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import io.javalin.Javalin
import io.javalin.http.Context
import io.javalin.http.HttpStatus
import io.javalin.http.bodyValidator
import mu.KotlinLogging
import org.apache.commons.lang3.exception.ExceptionUtils
import java.time.Instant
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.Executor
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.function.Supplier
import kotlin.concurrent.read
import kotlin.concurrent.write

class TaskDownloadHandler(
    private val configuration: Configuration,
    private val convExecutor: Executor,
    private val sseResponseBuilder: SseResponseBuilder,
    private val keepAliveHandler: KeepAliveHandler,
    private val searchMessagesHandler: SearchMessagesHandler,
    private val dataMeasurement: DataMeasurement,
) : JavalinHandler {
    private val tasksLock = ReentrantReadWriteLock()
    private val tasks: MutableMap<TaskID, TaskInformation> = HashMap()

    override fun setup(app: Javalin, context: JavalinContext) {
        app.post(DOWNLOAD_ROUTE, this::registerTask)
        app.get(TASK_STATUS_ROUTE, this::getTaskStatus)
        app.get(TASK_ROUTE, this::executeTask)
        app.delete(TASK_ROUTE, this::deleteTask)
    }

    private fun deleteTask(context: Context) {
        val taskID = TaskID.create(context.pathParam(TASK_ID))
        LOGGER.info { "Removing task $taskID" }
        val removed = tasksLock.write { tasks.remove(taskID) }
        if (removed == null) {
            LOGGER.error { "Task $taskID not found" }
            context.status(HttpStatus.NOT_FOUND)
                .json(ErrorMessage("task with id '${taskID.id}' is not found"))
        } else {
            LOGGER.info { "Task $taskID removed" }
            removed.onCanceled()
            context.status(HttpStatus.NO_CONTENT)
        }
    }

    private fun getTaskStatus(context: Context) {
        val taskID = TaskID.create(context.pathParam(TASK_ID))
        LOGGER.info { "Checking status for task $taskID" }
        val taskInfo = tasksLock.read { tasks[taskID] } ?: run {
            LOGGER.error { "Task $taskID not found" }
            context.status(HttpStatus.NOT_FOUND)
                .json(ErrorMessage("task with id '${taskID.id}' is not found"))
            return
        }
        context.status(HttpStatus.OK)
            .json(taskInfo.toTaskStatusResponse())
    }

    private fun executeTask(context: Context) {
        val taskID = TaskID.create(context.pathParam(TASK_ID))
        LOGGER.info { "Executing task $taskID" }
        val taskState: TaskState = tasksLock.write {
            val info = tasks[taskID] ?: run {
                return@write TaskState.NotFound
            }
            val queue = ArrayBlockingQueue<Supplier<SseEvent>>(configuration.responseQueueSize)
            val handler = HttpMessagesRequestHandler(
                queue, sseResponseBuilder, convExecutor, dataMeasurement,
                maxMessagesPerRequest = configuration.bufferPerQuery,
                responseFormats = info.request.responseFormats
                    ?: configuration.responseFormats,
            )
            if (!info.attachHandler(handler)) {
                return@write TaskState.AlreadyInProgress
            }
            TaskState.Ready(info, handler, queue)
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

    private sealed class TaskState {
        object AlreadyInProgress : TaskState()
        object NotFound : TaskState()
        data class Ready(
            val info: TaskInformation,
            val handler: HttpMessagesRequestHandler,
            val queue: ArrayBlockingQueue<Supplier<SseEvent>>,
        ) : TaskState()
    }

    private fun registerTask(context: Context) {
        val request = context.bodyValidator<CreateTaskRequest>()
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
        tasksLock.write { tasks[taskID] = TaskInformation(taskID, request.toGroupRequest()) }
        LOGGER.info { "Task $taskID registered" }

        context.status(HttpStatus.CREATED)
            .json(TaskIDResponse(taskID))
    }

    private data class TaskID(
        @field:JsonValue
        val id: String,
    ) {
        companion object {
            @JsonCreator
            @JvmStatic
            fun create(id: String): TaskID = TaskID(id)
        }
    }

    private class TaskIDResponse(
        val taskID: TaskID,
    )

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private class TaskStatusResponse(
        val taskID: TaskID,
        val status: TaskStatus,
        val errors: List<ErrorMessage> = emptyList(),
    )

    enum class TaskStatus {
        CREATED,
        EXECUTING,
        EXECUTING_WITH_ERRORS,
        COMPLETED,
        COMPLETED_WITH_ERRORS,
        CANCELED,
    }

    private class TaskInformation(
        val taskID: TaskID,
        val request: MessagesGroupRequest,
    ) : ProgressListener {
        private var _status: TaskStatus = TaskStatus.CREATED
        private val _errors: MutableList<ErrorHolder> = ArrayList()
        private var handler: CancelableResponseHandler? = null

        val status: TaskStatus
            get() = _status
        val errors: List<ErrorHolder>
            get() = _errors

        override fun onStart() {
            LOGGER.trace { "Task $taskID started" }
            _status = TaskStatus.EXECUTING
        }

        override fun onError(ex: Exception) {
            LOGGER.trace(ex) { "Task $taskID received an error" }
            _status = TaskStatus.EXECUTING_WITH_ERRORS
            _errors += ErrorHolder(
                ExceptionUtils.getMessage(ex),
                ExceptionUtils.getRootCauseMessage(ex),
            )
        }

        override fun onError(errorEvent: SseEvent.ErrorData) {
            LOGGER.trace { "Task $taskID received error event" }
            _status = TaskStatus.EXECUTING_WITH_ERRORS
            _errors += ErrorHolder(
                errorEvent.data.toString(Charsets.UTF_8)
            )
        }

        override fun onCompleted() {
            LOGGER.trace { "Task $taskID completed" }
            _status = when (_status) {
                TaskStatus.CANCELED ->
                    TaskStatus.CANCELED

                TaskStatus.EXECUTING_WITH_ERRORS ->
                    TaskStatus.COMPLETED_WITH_ERRORS

                else ->
                    TaskStatus.COMPLETED
            }
        }

        override fun onCanceled() {
            LOGGER.trace { "Task $taskID canceled" }
            _status = when (_status) {
                TaskStatus.EXECUTING_WITH_ERRORS ->
                    TaskStatus.EXECUTING_WITH_ERRORS

                TaskStatus.COMPLETED_WITH_ERRORS, TaskStatus.COMPLETED ->
                    _status

                else ->
                    TaskStatus.CANCELED
            }
            handler?.also {
                if (it.isAlive) {
                    it.cancel()
                }
            }
        }

        fun attachHandler(handler: CancelableResponseHandler): Boolean {
            LOGGER.trace { "Attaching handler to task $taskID" }
            if (this.handler != null) {
                return false
            }
            this.handler = handler
            return true
        }

        companion object {
            private val LOGGER = KotlinLogging.logger(TaskInformation::class.qualifiedName!!)
        }
    }

    private class ErrorHolder(
        val message: String,
        val cause: String? = null,
    )

    private fun TaskInformation.toTaskStatusResponse(): TaskStatusResponse =
        TaskStatusResponse(
            taskID = taskID,
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
            bookId = bookId,
            responseFormats = responseFormats.ifEmpty { configuration.responseFormats },
            includeStreams = streams.asSequence().flatMap { it.toProviderMessageStreams() }
                .toSet(),
            searchDirection = searchDirection,
            limit = limit,
        )
    }

    private fun MessageStream.toProviderMessageStreams(): Sequence<ProviderMessageStream> {
        return directions.asSequence().map { ProviderMessageStream(sessionAlias, it) }
    }

    private class CreateTaskRequest(
        val resource: Resource,
        val bookId: BookId,
        @field:JsonDeserialize(using = CustomMillisOrNanosInstantDeserializer::class)
        val startTimestamp: Instant,
        @field:JsonDeserialize(using = CustomMillisOrNanosInstantDeserializer::class)
        val endTimestamp: Instant,
        val groups: Set<String>,
        val responseFormats: Set<ResponseFormat> = emptySet(),
        val limit: Int? = null,
        val streams: List<MessageStream> = emptyList(),
        val searchDirection: SearchDirection = SearchDirection.next,
    )

    private class MessageStream(
        val sessionAlias: String,
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