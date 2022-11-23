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

package com.exactpro.th2.lwdataprovider.grpc

import com.exactpro.th2.common.message.toJavaDuration
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.dataprovider.lw.grpc.LoadedStatistic
import com.exactpro.th2.dataprovider.lw.grpc.MessageGroupsQueueSearchRequest
import com.exactpro.th2.dataprovider.lw.grpc.MessageGroupsSearchRequest
import com.exactpro.th2.dataprovider.lw.grpc.QueueProviderServiceGrpc
import com.exactpro.th2.lwdataprovider.ResponseHandler
import com.exactpro.th2.lwdataprovider.entities.requests.QueueMessageGroupsRequest
import com.exactpro.th2.lwdataprovider.handlers.LoadStatistic
import com.exactpro.th2.lwdataprovider.handlers.QueueMessagesHandler
import io.grpc.Context
import io.grpc.Status
import io.grpc.stub.StreamObserver
import mu.KotlinLogging

class QueueGrpcProvider(
    private val messagesHandler: QueueMessagesHandler,
) : QueueProviderServiceGrpc.QueueProviderServiceImplBase() {
    override fun searchMessageGroups(
        request: MessageGroupsQueueSearchRequest,
        responseObserver: StreamObserver<LoadedStatistic>,
    ) {
        try {
            val internalRequest = request.run {
                QueueMessageGroupsRequest.create(
                    messageGroupList.mapTo(hashSetOf()) { it.name },
                    if (hasStartTimestamp()) startTimestamp.toInstant() else null,
                    if (hasEndTimestamp()) endTimestamp.toInstant() else null,
                    if (hasSyncInterval()) syncInterval.toJavaDuration() else null,
                    keepAlive,
                    externalQueue,
                )
            }
            messagesHandler.requestMessageGroups(
                internalRequest,
                LoadStatisticResponseHandler(responseObserver),
            )
        } catch (ex: IllegalArgumentException) {
            LOGGER.error(ex) { "invalid request ${request.toJson()}" }
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(ex.message).asRuntimeException())
        } catch (ex: Exception) {
            LOGGER.error(ex) { "cannot request groups for ${request.toJson()}" }
            responseObserver.onError(Status.INTERNAL.withDescription(ex.message).asRuntimeException())
        }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }
}

private class LoadStatisticResponseHandler(
    private val responseObserver: StreamObserver<LoadedStatistic>
) : ResponseHandler<LoadStatistic> {
    @Volatile
    private var hasError = false
    override val isAlive: Boolean
        get() = !Context.current().isCancelled

    override fun handleNext(data: LoadStatistic) {
        if (hasError) return
        responseObserver.onNext(data.toGrpcResponse())
    }

    override fun complete() {
        if (hasError) return
        responseObserver.onCompleted()
    }

    override fun writeErrorMessage(text: String) {
        responseObserver.onError(Status.INTERNAL.withDescription(text).asRuntimeException())
        hasError = true
    }

}

private fun LoadStatistic.toGrpcResponse(): LoadedStatistic {
    val builder = LoadedStatistic.newBuilder()
    messagesByGroup.forEach { (group, count) ->
        builder.addStat(
            LoadedStatistic.GroupStat.newBuilder().setGroup(
                MessageGroupsSearchRequest.Group.newBuilder().setName(group)
            ).setCount(count)
        )
    }
    return builder.build()
}
