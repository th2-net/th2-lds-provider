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

package com.exactpro.th2.lwdataprovider.producers

import com.exactpro.cradle.messages.StoredMessageId
import com.exactpro.cradle.testevents.*
import com.exactpro.th2.lwdataprovider.entities.internal.ProviderEventId
import com.exactpro.th2.lwdataprovider.entities.responses.BaseEventEntity
import mu.KotlinLogging
import java.util.Collections
import java.util.stream.Collectors

class EventProducer() {

    companion object {
        private val logger = KotlinLogging.logger { }

        fun fromEventMetadata(
            storedEvent: StoredTestEventMetadata
        ): BaseEventEntity {
            val batchId = storedEvent.batchMetadata?.id

            return BaseEventEntity(
                storedEvent,
                ProviderEventId(batchId, storedEvent.id),
                batchId,
                storedEvent.parentId?.let { ProviderEventId(null, storedEvent.parentId) }
            )
        }

        fun fromSingleEvent(storedEvent: StoredTestEventSingle): BaseEventEntity {
            return BaseEventEntity(
                "event",
                ProviderEventId(null, storedEvent.id),
                null,
                false,
                storedEvent.name ?: "",
                storedEvent.type ?: "",
                storedEvent.endTimestamp,
                storedEvent.startTimestamp,
                storedEvent.parentId?.let { ProviderEventId(null, storedEvent.parentId) },
                storedEvent.isSuccess,
                loadAttachedMessages(storedEvent.messageIds),
                storedEvent.content.toString(Charsets.UTF_8),
            )
        }

        fun fromBatchEvent(
            storedEvent: BatchedStoredTestEvent,
            batch: StoredTestEventBatch,
        ): BaseEventEntity {
            return BaseEventEntity(
                "event",
                ProviderEventId(storedEvent.batchId, storedEvent.id),
                storedEvent.batchId,
                true,
                storedEvent.name ?: "",
                storedEvent.type ?: "",
                storedEvent.endTimestamp,
                storedEvent.startTimestamp,
                storedEvent.parentId?.let {
                    ProviderEventId(
                        batch.getTestEvent(storedEvent.parentId)?.let { batch.id /*if the parent in the current batch*/ },
                        storedEvent.parentId
                    )
                },
                storedEvent.isSuccess,
                loadAttachedMessages(storedEvent.messageIds),
                storedEvent.content.toString(Charsets.UTF_8),
            )
        }

        private fun loadAttachedMessages(messageIds: Collection<StoredMessageId>?): Set<String> {
            return messageIds?.asSequence()?.map { t -> t.toString() }?.toSet() ?: Collections.emptySet()
        }
    }

}
