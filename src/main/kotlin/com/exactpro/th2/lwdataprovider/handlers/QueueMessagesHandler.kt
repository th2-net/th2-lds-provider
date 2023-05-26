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

package com.exactpro.th2.lwdataprovider.handlers

import com.exactpro.cradle.BookId
import com.exactpro.cradle.messages.GroupedMessageFilter
import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.MessageGroupBatchMetadata
import com.exactpro.th2.common.message.plusAssign
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.schema.message.QueueAttribute
import com.exactpro.th2.lwdataprovider.ProviderStreamInfo
import com.exactpro.th2.lwdataprovider.ResponseHandler
import com.exactpro.th2.lwdataprovider.db.CancellationReason
import com.exactpro.th2.lwdataprovider.db.CradleGroupRequest
import com.exactpro.th2.lwdataprovider.db.CradleMessageExtractor
import com.exactpro.th2.lwdataprovider.db.DataMeasurement
import com.exactpro.th2.lwdataprovider.db.MessageDataSink
import com.exactpro.th2.lwdataprovider.entities.requests.QueueMessageGroupsRequest
import com.exactpro.th2.lwdataprovider.grpc.toRawMessage
import com.exactpro.th2.lwdataprovider.handlers.util.BookGroup
import com.exactpro.th2.lwdataprovider.handlers.util.GroupParametersHolder
import com.exactpro.th2.lwdataprovider.handlers.util.computeNewParametersForGroupRequest
import mu.KotlinLogging
import java.time.Instant
import java.util.concurrent.Executor

class QueueMessagesHandler(
    private val extractor: CradleMessageExtractor,
    private val dataMeasurement: DataMeasurement,
    private val router: MessageRouter<MessageGroupBatch>,
    private val batchMaxSize: Int,
    private val useAttributes: Boolean,
    private val executor: Executor,
) {
    fun requestMessageGroups(
        request: QueueMessageGroupsRequest,
        handler: ResponseHandler<LoadStatistic>,
    ) {
        if (request.groupsByBook.isEmpty()) {
            handler.complete()
            return
        }
        executor.execute {
            val param = CradleGroupRequest(false)
            val streamInfo = ProviderStreamInfo()
            val batchMetadata = MessageGroupBatchMetadata.newBuilder().setExternalQueue(request.externalQueue).build()
            createSink(handler, streamInfo, batchMaxSize) { batch, attribute ->
                val toSend: MessageGroupBatch = batch.setMetadata(batchMetadata)
                    .build()
                if (request.sendRawDirectly) {
                    router.sendExclusive(request.externalQueue, toSend)
                }
                if (request.rawOnly) {
                    return@createSink
                }
                if (useAttributes) {
                    router.send(toSend, attribute, QueueAttribute.RAW.value)
                } else {
                    router.sendAll(toSend, QueueAttribute.RAW.value)
                }
            }.use { sink ->
                try {
                    extractor.getGroupsWithSyncInterval(
                        request.groupsByBook.toFilters(
                            request.startTimestamp,
                            request.endTimestamp,
                        ) { param },
                        request.syncInterval,
                        sink,
                        dataMeasurement,
                    )

                    if (request.keepAlive) {
                        val groupSize = request.groupsByBook.size
                        val allLoaded = hashSetOf<String>()
                        do {
                            sink.canceled?.apply {
                                LOGGER.info { "request canceled: $message" }
                                return@execute
                            }
                            val newParams: Map<BookGroup, GroupParametersHolder> = computeNewParametersForGroupRequest(
                                request.groupsByBook,
                                request.startTimestamp,
                                request.endTimestamp,
                                param,
                                allLoaded,
                                streamInfo,
                                extractor
                            )
                            sink.canceled?.apply {
                                LOGGER.info { "request canceled: $message" }
                                return@execute
                            }
                            extractor.getGroupsWithSyncInterval(
                                newParams.mapValues { (bookGroup, holder) ->
                                    groupedMessageFilter(
                                        bookGroup.bookId,
                                        bookGroup.group,
                                        holder.newStartTime,
                                        request.endTimestamp,
                                    ) to holder.params
                                },
                                request.syncInterval,
                                sink,
                                dataMeasurement,
                            )
                        } while (allLoaded.size != groupSize)
                    }
                } catch (ex: Exception) {
                    LOGGER.error(ex) { "cannot load groups for request $request" }
                    sink.onError(ex)
                }
            }
        }
    }

    private fun createSink(
        handler: ResponseHandler<LoadStatistic>,
        streamInfo: ProviderStreamInfo,
        batchMaxSize: Int,
        onBatch: (MessageGroupBatch.Builder, String) -> Unit,
    ): MessageDataSink<String, StoredMessage> {
        return QueueMessageDataSink(
            handler,
            streamInfo,
            batchMaxSize,
            onBatch,
        )
    }

    private inline fun Map<BookId, Set<String>>.toFilters(
        start: Instant,
        end: Instant,
        crossinline paramSupplier: (BookGroup) -> CradleGroupRequest,
    ): Map<BookGroup, Pair<GroupedMessageFilter, CradleGroupRequest>> {
        return asSequence().flatMap { (bookId, groups) ->
            groups.ifEmpty { extractor.getAllGroups(bookId) }.asSequence().map { group ->
                val filter = groupedMessageFilter(bookId, group, start, end)
                val bookGroup = BookGroup(group, bookId)
                bookGroup to (filter to paramSupplier(bookGroup))
            }
        }.toMap()
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }
}

private fun groupedMessageFilter(
    bookId: BookId,
    group: String,
    start: Instant,
    end: Instant
): GroupedMessageFilter = GroupedMessageFilter.builder()
    .groupName(group)
    .bookId(bookId)
    .timestampFrom().isGreaterThanOrEqualTo(start)
    .timestampTo().isLessThan(end)
    .build()

private class QueueMessageDataSink(
    private val handler: ResponseHandler<LoadStatistic>,
    private val streamInfo: ProviderStreamInfo,
    private val batchMaxSize: Int,
    private val onBatch: (MessageGroupBatch.Builder, String) -> Unit,
) : MessageDataSink<String, StoredMessage> {
    private val countByGroup = hashMapOf<BookGroup, Long>()
    private val batchBuilder = MessageGroupBatch.newBuilder()
    private var lastMarker: String? = null

    override fun onNext(marker: String, data: StoredMessage) {
        countByGroup.merge(BookGroup(marker, data.bookId), 1, Long::plus)
        val curLastMarker = lastMarker
        if (curLastMarker?.equals(marker) == false) {
            processBatch(curLastMarker)
        }
        lastMarker = marker
        batchBuilder.addGroupsBuilder() += data.toRawMessage()
        if (batchBuilder.groupsList.sumOf { it.messagesCount } >= batchMaxSize) {
            processBatch(marker)
        }
        streamInfo.registerMessage(data.id, data.timestamp, marker)
    }

    override val canceled: CancellationReason?
        get() = if (handler.isAlive) null else CancellationReason("user request is canceled")

    override fun onError(message: String, id: String?, batchId: String?) {
        handler.writeErrorMessage(message, id, batchId)
    }

    override fun completed() {
        lastMarker?.also { processBatch(it) }

        handler.handleNext(LoadStatistic(countByGroup))
        handler.complete()
    }

    private fun processBatch(marker: String) {
        onBatch(batchBuilder, marker)
        batchBuilder.clear()
    }

}

class LoadStatistic(
    val messagesByGroup: Map<BookGroup, Long>,
)
