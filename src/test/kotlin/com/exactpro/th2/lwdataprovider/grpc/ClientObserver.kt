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
import io.grpc.stub.ClientCallStreamObserver
import io.grpc.stub.ClientResponseObserver
import mu.KotlinLogging
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

class ClientObserver(
    private val initialGrpcRequest: Int,
    private val periodicalGrpcRequest: Int,
) : ClientResponseObserver<MessageGroupsSearchRequest, MessageGroupsSearchResponse> {
    private val done = CountDownLatch(1)
    private val sampler = Sampler("client (ON)", ThroughputTest.SAMPLING_FREQUENCY)

    @Volatile
    private lateinit var requestStream: ClientCallStreamObserver<MessageGroupsSearchRequest>
    private val counter = AtomicInteger()

    override fun beforeStart(requestStream: ClientCallStreamObserver<MessageGroupsSearchRequest>) {
        LOGGER.debug { "beforeStart has been called" }
        this.requestStream = requestStream
        // Set up manual flow control for the response stream. It feels backwards to configure the response
        // stream's flow control using the request stream's observer, but this is the way it is.
        requestStream.disableAutoRequestWithInitial(initialGrpcRequest)
    }

    override fun onNext(value: MessageGroupsSearchResponse) {
        LOGGER.debug { "onNext has been called $value" }
        if (periodicalGrpcRequest > 0) {
            if (counter.incrementAndGet() % periodicalGrpcRequest == 0) {
                requestStream.request(periodicalGrpcRequest)
            }
        }
        sampler.inc(value.collection.messagesCount)
    }

    override fun onError(t: Throwable) {
        LOGGER.error (t) { "onError has been called" }
        done.countDown()
        sampler.complete()
    }

    override fun onCompleted() {
        LOGGER.info { "onCompleted has been called" }
        done.countDown()
        sampler.complete()
    }

    fun await() {
        done.await()
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }
}