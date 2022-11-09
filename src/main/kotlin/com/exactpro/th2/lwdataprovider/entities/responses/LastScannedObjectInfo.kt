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

package com.exactpro.th2.lwdataprovider.entities.responses

import java.time.Instant
import java.util.concurrent.atomic.AtomicLong

data class LastScannedObjectInfo(var id: String = "", var timestamp: Long = 0, var scanCounter: Long = 0) {

    fun update(lastTimestamp: Instant) {
        timestamp = lastTimestamp.toEpochMilli()
    }

    fun update(event: BaseEventEntity, scanCnt: Long) {
        update(event.id.toString(), event.startTimestamp, scanCnt)
    }

    fun update(message: ProviderMessage, scanCnt: Long) {
        update(message.id, message.timestamp, scanCnt)
    }

    fun update(id: String, timestamp: Instant?, scanCnt: Long) {
        update(id, timestamp?.toEpochMilli(), scanCnt)
    }

    fun update(id: String, timestamp: Long?, scanCnt: Long) {
        this.id = id
        this.timestamp = timestamp?: 0
        this.scanCounter = scanCnt
    }
}