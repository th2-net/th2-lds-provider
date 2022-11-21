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

import com.exactpro.cradle.messages.StoredMessage
import com.exactpro.th2.common.grpc.MessageGroupBatch
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
import com.exactpro.th2.lwdataprovider.handlers.util.computeNewParametersForGroupRequest
import mu.KotlinLogging
import java.util.concurrent.Executor

class QueueMessagesHandler(
    private val extractor: CradleMessageExtractor,
    private val dataMeasurement: DataMeasurement,
    private val router: MessageRouter<MessageGroupBatch>,
    private val batchMaxSize: Int,
    private val executor: Executor,
) {
    fun requestMessageGroups(
        request: QueueMessageGroupsRequest,
        handler: ResponseHandler<LoadStatistic>,
    ) {
        if (request.groups.isEmpty()) {
            handler.complete()
            return
        }
        executor.execute {
            val param = CradleGroupRequest(request.startTimestamp, request.endTimestamp, false)
            val streamInfo = ProviderStreamInfo()
            createSink(handler, streamInfo, batchMaxSize) { bath, attribute ->
                router.send(bath, attribute, request.externalQueue, QueueAttribute.RAW.value)
            }.use { sink ->
                try {
                    extractor.getGroupsWithSyncInterval(
                        request.groups.associateWith { param },
                        request.syncInterval,
                        sink,
                        dataMeasurement,
                    )

                    if (request.keepAlive) {
                        val groupSize = request.groups.size
                        val allLoaded = hashSetOf<String>()
                        do {
                            sink.canceled?.apply {
                                LOGGER.info { "request canceled: $message" }
                                return@execute
                            }
                            val newParams = computeNewParametersForGroupRequest(
                                request.groups,
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
                                newParams,
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
        onBatch: (MessageGroupBatch, String) -> Unit,
    ): MessageDataSink<String, StoredMessage> {
        return QueueMessageDataSink(
            handler,
            streamInfo,
            batchMaxSize,
            onBatch,
        )
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }
}

private class QueueMessageDataSink(
    private val handler: ResponseHandler<LoadStatistic>,
    private val streamInfo: ProviderStreamInfo,
    private val batchMaxSize: Int,
    private val onBatch: (MessageGroupBatch, String) -> Unit,
) : MessageDataSink<String, StoredMessage> {
    private val countByGroup = hashMapOf<String, Long>()
    private val batchBuilder = MessageGroupBatch.newBuilder()
    private var lastMarker: String? = null

    override fun onNext(marker: String, data: StoredMessage) {
        countByGroup.merge(marker, 1, Long::plus)
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

    override fun onError(message: String) {
        handler.writeErrorMessage(message)
    }

    override fun completed() {
        lastMarker?.also { processBatch(it) }

        handler.handleNext(LoadStatistic(countByGroup))
        handler.complete()
    }

    private fun processBatch(marker: String) {
        onBatch(batchBuilder.build(), marker)
        batchBuilder.clear()
    }

}

class LoadStatistic(
    val messagesByGroup: Map<String, Long>,
)
