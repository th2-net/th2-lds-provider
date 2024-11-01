/*
 * Copyright 2023-2024 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.lwdataprovider.workers

import com.exactpro.th2.lwdataprovider.CancelableResponseHandler
import com.exactpro.th2.lwdataprovider.SseEvent
import com.exactpro.th2.lwdataprovider.entities.requests.MessagesGroupRequest
import com.exactpro.th2.lwdataprovider.entities.requests.SseEventSearchRequest
import com.exactpro.th2.lwdataprovider.http.listener.ProgressListener
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonValue
import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.commons.lang3.exception.ExceptionUtils
import java.time.Duration
import java.time.Instant
import java.util.concurrent.locks.ReentrantReadWriteLock
import javax.annotation.concurrent.NotThreadSafe
import kotlin.concurrent.read
import kotlin.concurrent.write

class TaskManager : AutoCloseable, TimeoutChecker {
    private val tasksLock = ReentrantReadWriteLock()
    private val tasks: MutableMap<TaskID, TaskInformation> = HashMap()

    operator fun get(taskID: TaskID): TaskInformation? =
        tasksLock.read { tasks[taskID] }

    operator fun set(taskID: TaskID, information: TaskInformation): Unit =
        tasksLock.write { tasks[taskID] = information }

    fun remove(taskID: TaskID): TaskInformation? =
        tasksLock.write { internalRemove(taskID) }

    fun <T> execute(taskID: TaskID, action: (TaskInformation?) -> T): T =
        tasksLock.write { action(tasks[taskID]) }

    override fun removeOlderThen(timeout: Long): Long {
        val currentTime = Instant.now()
        val (tasksToCleanup: Map<TaskID, TaskInformation>, minCreationTime: Instant) = tasksLock.read {
            val mitTime: Instant = tasks.values.minOfOrNull { info ->
                info.completionTime?.let { minOf(it, info.creationTime) }
                    ?: info.creationTime
            } ?: currentTime

            tasks.filter { (_, info) ->
                createdButNotStarted(info, currentTime, timeout)
                        || completed(info, currentTime, timeout)
            } to mitTime
        }
        if (tasksToCleanup.isNotEmpty()) {
            tasksLock.write {
                for ((id, info) in tasksToCleanup) {
                    LOGGER.info { "Canceling task $id in status ${info.status} due to timeout in $timeout mls" }
                    internalRemove(id)
                }
            }
        }
        return minCreationTime.toEpochMilli()
    }

    private fun internalRemove(taskID: TaskID) = tasks.remove(taskID)?.apply(TaskInformation::onCanceled)

    private fun completed(info: TaskInformation, currentTime: Instant, timeout: Long): Boolean {
        return info.completionTime.let { completedAt ->
            completedAt != null && checkDiff(currentTime, timeout, completedAt)
        }
    }

    private fun createdButNotStarted(info: TaskInformation, currentTime: Instant, timeout: Long) =
        info.status == TaskStatus.CREATED && checkDiff(currentTime, timeout, info.creationTime)

    private fun checkDiff(currentTime: Instant, timeout: Long, taskTime: Instant) =
        Duration.between(taskTime, currentTime).abs().toMillis() > timeout

    override fun close() {
        tasksLock.write {
            tasks.forEach { (id, taskInfo) ->
                LOGGER.info { "Closing task $id in status ${taskInfo.status}" }
                taskInfo.onCanceled()
            }
            tasks.clear()
        }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }
}

data class TaskID(
    @field:JsonValue
    val id: String,
) {
    companion object {
        @JsonCreator
        @JvmStatic
        fun create(id: String): TaskID = TaskID(id)
    }
}

enum class TaskStatus {
    CREATED,
    EXECUTING,
    EXECUTING_WITH_ERRORS,
    COMPLETED,
    COMPLETED_WITH_ERRORS,
    CANCELED,
    CANCELED_WITH_ERRORS,
}

@NotThreadSafe
sealed class TaskInformation(
    val taskID: TaskID,
) : ProgressListener {
    val creationTime: Instant = Instant.now()
    @Volatile
    var completionTime: Instant? = null
        private set
    @Volatile
    private var _status: TaskStatus = TaskStatus.CREATED
    private val _errors: MutableList<ErrorHolder> = ArrayList()
    @Volatile
    private var handler: CancelableResponseHandler? = null

    val status: TaskStatus
        get() = _status
    val errors: List<ErrorHolder>
        get() = _errors.toList()

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

            TaskStatus.CREATED,
            TaskStatus.EXECUTING,
            TaskStatus.COMPLETED,
            TaskStatus.COMPLETED_WITH_ERRORS,
            TaskStatus.CANCELED_WITH_ERRORS ->
                TaskStatus.COMPLETED
        }
        completionTime = Instant.now()
    }

    override fun onCanceled() {
        LOGGER.trace { "Task $taskID canceled" }
        _status = when (_status) {
            TaskStatus.EXECUTING_WITH_ERRORS ->
                TaskStatus.CANCELED_WITH_ERRORS

            TaskStatus.COMPLETED_WITH_ERRORS,
            TaskStatus.COMPLETED ->
                _status

            TaskStatus.CREATED,
            TaskStatus.EXECUTING,
            TaskStatus.CANCELED,
            TaskStatus.CANCELED_WITH_ERRORS ->
                TaskStatus.CANCELED
        }
        handler?.also {
            if (it.isAlive) {
                it.cancel()
            }
        }
        completionTime = Instant.now()
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

@NotThreadSafe
class MessageTaskInfo(
    taskID: TaskID,
    val request: MessagesGroupRequest,
) : TaskInformation(taskID)

@NotThreadSafe
class EventTaskInfo(
    taskID: TaskID,
    val request: SseEventSearchRequest,
) : TaskInformation(taskID)

class ErrorHolder(
    val message: String,
    val cause: String? = null,
)