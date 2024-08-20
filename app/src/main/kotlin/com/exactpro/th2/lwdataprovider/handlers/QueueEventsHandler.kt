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

package com.exactpro.th2.lwdataprovider.handlers

import com.exactpro.cradle.BookId
import com.exactpro.th2.common.event.EventUtils
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventStatus
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.lwdataprovider.ResponseHandler
import com.exactpro.th2.lwdataprovider.db.CancellationReason
import com.exactpro.th2.lwdataprovider.db.CradleEventExtractor
import com.exactpro.th2.lwdataprovider.db.EventDataSink
import com.exactpro.th2.lwdataprovider.entities.requests.QueueEventsScopeRequest
import com.exactpro.th2.lwdataprovider.entities.responses.Event
import com.exactpro.th2.lwdataprovider.handlers.util.BookScope
import com.google.protobuf.UnsafeByteOperations
import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.commons.lang3.StringUtils.isBlank
import java.util.concurrent.Executor
import com.exactpro.th2.common.grpc.Event as CommonGrpcEvent

class QueueEventsHandler(
    private val extractor: CradleEventExtractor,
    private val router: MessageRouter<EventBatch>,
    private val batchMaxSize: Int,
    private val executor: Executor,
) {
    fun requestEvents(
        request: QueueEventsScopeRequest,
        handler: ResponseHandler<EventsLoadStatistic>,
    ) {
        if (request.scopesByBook.isEmpty()) {
            handler.complete()
            return
        }
        executor.execute {
            EventQueueDataSink(
                handler,
                batchMaxSize,
            ) {
                if (isBlank(request.externalQueue)) {
                    router.sendAll(it)
                } else {
                    router.sendExclusive(request.externalQueue, it)
                }
            }.use { sink ->
                try {
                    with(request) {
                        extractor.getEventsWithSyncInterval(
                            startTimestamp,
                            endTimestamp,
                            syncInterval,
                            scopesByBook.mapValues { (bookId, scopes) ->
                                scopes.ifEmpty { extractor.getAllEventsScopes(bookId) }
                            },
                            sink
                        )
                    }
                } catch (ex: Exception) {
                    LOGGER.error(ex) { "cannot execute request $request" }
                    sink.onError(ex)
                }
            }
        }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }
}

class EventsLoadStatistic(val countByScope: Map<BookScope, Long>)

private class EventQueueDataSink(
    private val handler: ResponseHandler<EventsLoadStatistic>,
    private val maxBatchSize: Int,
    private val onBatch: (EventBatch) -> Unit,
) : EventDataSink<Event> {
    private val countByScope = hashMapOf<BookScope, Long>()
    private val batchBuilder = EventBatch.newBuilder()
    override val canceled: CancellationReason?
        get() = when {
            !handler.isAlive -> CancellationReason("request canceled by user")
            else -> null
        }

    override fun onNext(data: Event) {
        countByScope.merge(BookScope(data.scope, BookId(data.bookId)), 1L, Long::plus)
        batchBuilder.addEvents(data.toGrpc())
        if (batchBuilder.eventsCount >= maxBatchSize) {
            processBatch(batchBuilder)
        }
    }

    override fun onError(message: String, id: String?, batchId: String?) {
        handler.writeErrorMessage(message, id, batchId)
        handler.complete()
    }

    override fun completed() {
        processBatch(batchBuilder)
        handler.handleNext(
            EventsLoadStatistic(countByScope)
        )
        handler.complete()
    }

    private fun processBatch(builder: EventBatch.Builder) {
        if (builder.eventsCount == 0) return
        onBatch(builder.build())
        builder.clear()
    }

}

private fun Event.toGrpc(): CommonGrpcEvent {
    return CommonGrpcEvent.newBuilder()
        .setId(EventUtils.toEventID(startTimestamp, bookId, scope, shortEventId))
        .setName(eventName)
        .setType(eventType)
        .setStatus(if (successful) EventStatus.SUCCESS else EventStatus.FAILED)
        .setBody(UnsafeByteOperations.unsafeWrap(body.toByteArray(Charsets.UTF_8)))
        .also { event ->
            endTimestamp?.also { event.endTimestamp = it.toTimestamp() }
            parentEventId?.also { event.parentId = Event.convertToEventIdProto(it) }
            if (attachedMessageIds.isNotEmpty()) {
                event.addAllAttachedMessageIds(Event.convertMessageIdToProto(attachedMessageIds))
            }
        }
        .build()
}
