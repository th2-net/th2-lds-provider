/*
 *  Copyright 2022 Exactpro (Exactpro Systems Limited)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.exactpro.th2.lwdataprovider.grpc

import com.exactpro.th2.dataprovider.grpc.MessageGroupsSearchRequest
import com.exactpro.th2.dataprovider.grpc.MessageGroupsSearchResponse
import com.exactpro.th2.lwdataprovider.grpc.ThroughputTest.Companion.SAMPLING_FREQUENCY
import io.grpc.stub.ServerCallStreamObserver
import io.grpc.stub.StreamObserver
import mu.KotlinLogging
import java.util.concurrent.ConcurrentHashMap

class ManualServer (
    sequence: Sequence<MessageGroupsSearchResponse>,
) : AbstractServer(sequence) {
    override val sampler = Sampler("server (ON)", SAMPLING_FREQUENCY)
    private val activeObservers: MutableSet<StreamObserver<*>> = ConcurrentHashMap.newKeySet()

    override fun searchMessageGroups(
        request: MessageGroupsSearchRequest,
        responseObserver: StreamObserver<MessageGroupsSearchResponse>
    ) {
        check(activeObservers.add(responseObserver)) {
            "Active observers set has already contain a new observer"
        }
        (responseObserver as ServerCallStreamObserver<MessageGroupsSearchResponse>).apply {
            responseObserver.setOnReadyHandler {
                while (responseObserver.isReady && inProcess) {
                   send(responseObserver)
                }
                if (!inProcess) {
                    LOGGER.info { "server complete" }
                    if (activeObservers.remove(responseObserver)) {
                        responseObserver.onCompleted()
                    }
                    done.countDown()
                }
            }
        }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }
}