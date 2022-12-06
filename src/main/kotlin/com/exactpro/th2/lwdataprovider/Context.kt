/*******************************************************************************
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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
 ******************************************************************************/

package com.exactpro.th2.lwdataprovider

import com.exactpro.cradle.CradleManager
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.lwdataprovider.configuration.Configuration
import com.exactpro.th2.lwdataprovider.db.CradleEventExtractor
import com.exactpro.th2.lwdataprovider.db.CradleMessageExtractor
import com.exactpro.th2.lwdataprovider.db.DataMeasurement
import com.exactpro.th2.lwdataprovider.db.GeneralCradleExtractor
import com.exactpro.th2.lwdataprovider.handlers.GeneralCradleHandler
import com.exactpro.th2.lwdataprovider.handlers.QueueEventsHandler
import com.exactpro.th2.lwdataprovider.handlers.QueueMessagesHandler
import com.exactpro.th2.lwdataprovider.handlers.SearchEventsHandler
import com.exactpro.th2.lwdataprovider.handlers.SearchMessagesHandler
import com.exactpro.th2.lwdataprovider.workers.KeepAliveHandler
import com.exactpro.th2.lwdataprovider.workers.TimerWatcher
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

@Suppress("MemberVisibilityCanBePrivate")
class Context(
    val configuration: Configuration,

    val jacksonMapper: ObjectMapper = jacksonObjectMapper()
        .enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY)
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .disable(SerializationFeature.INDENT_OUTPUT),

    val cradleManager: CradleManager,
    val messageRouter: MessageRouter<MessageGroupBatch>,
    val eventRouter: MessageRouter<EventBatch>,
    val keepAliveHandler: KeepAliveHandler = KeepAliveHandler(configuration),
    val mqDecoder: RabbitMqDecoder = RabbitMqDecoder(messageRouter, configuration.maxBufferDecodeQueue, configuration.codecUsePinAttributes),

    val timeoutHandler: TimerWatcher = TimerWatcher(mqDecoder, configuration),
    val cradleEventExtractor: CradleEventExtractor = CradleEventExtractor(cradleManager),
    val cradleMsgExtractor: CradleMessageExtractor = CradleMessageExtractor(configuration.groupRequestBuffer, cradleManager),
    val generalCradleExtractor: GeneralCradleExtractor = GeneralCradleExtractor(cradleManager),
    val pool: ExecutorService = Executors.newFixedThreadPool(configuration.execThreadPoolSize),

    val searchMessagesHandler: SearchMessagesHandler = SearchMessagesHandler(
        cradleMsgExtractor,
        mqDecoder,
        pool,
        configuration,
    ),
    val searchEventsHandler: SearchEventsHandler = SearchEventsHandler(cradleEventExtractor, pool),
    val dataMeasurement: DataMeasurement = DataMeasurementImpl,
    val queueMessageHandler: QueueMessagesHandler = QueueMessagesHandler(
        cradleMsgExtractor,
        dataMeasurement,
        messageRouter,
        configuration.batchSize,
        pool,
    ),
    val queueEventsHandler: QueueEventsHandler = QueueEventsHandler(
        cradleEventExtractor,
        eventRouter,
        configuration.batchSize,
        pool,
    ),
    val generalCradleHandler: GeneralCradleHandler = GeneralCradleHandler(generalCradleExtractor),
)
