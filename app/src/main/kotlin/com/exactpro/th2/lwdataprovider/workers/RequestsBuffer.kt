/*
 * Copyright 2022-2023 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage

interface RequestsBuffer {
    /**
     * Notifies the [RequestsBuffer] that the batch with first group [RequestId] is received.
     * Used to stop all internal measurements for that batch
     */
    fun batchReceived(firstRequestId: RequestId)
    /**
     * @param id [RequestId] for which the response was received
     * @param response the supplier function that will be invoked it the [id] is in the decoding queue
     */
    fun responseProtoReceived(id: RequestId, response: () -> List<Message>)
    fun responseTransportReceived(id: RequestId, response: () -> List<ParsedMessage>)
    fun bulkResponsesProtoReceived(responses: Map<RequestId, () -> List<Message>>)
    fun bulkResponsesTransportReceived(responses: Map<RequestId, () -> List<ParsedMessage>>)

    /**
     * @return the sending time of the oldest message request or current time in epoch millis
     */
    fun removeOlderThan(timeout: Long): Long
}