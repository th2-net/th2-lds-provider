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

package com.exactpro.th2.lwdataprovider.entities.internal

import com.exactpro.cradle.testevents.StoredTestEventId

class ProviderEventId(val batchId: StoredTestEventId?, val eventId: StoredTestEventId) {
    companion object {
        const val divider = ">" // FIXME: like in the main provider it is a temporal solution
    }

    constructor(id: String) : this(
        batchId = if (id.contains(divider)) id.split(divider).getOrNull(0)?.let { StoredTestEventId.fromString(it) } else null,
        eventId = id.split(divider).getOrNull(1)?.let { StoredTestEventId.fromString(it) } ?: StoredTestEventId.fromString(id)
    )

    override fun toString(): String {
        return (batchId?.toString()?.let { it + divider } ?: "") + eventId.toString()
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as ProviderEventId

        if (batchId != other.batchId) return false
        if (eventId != other.eventId) return false

        return true
    }

    override fun hashCode(): Int {
        var result = batchId?.hashCode() ?: 0
        result = 31 * result + eventId.hashCode()
        return result
    }


}
