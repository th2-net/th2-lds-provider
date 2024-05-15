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

package com.exactpro.th2.lwdataprovider.filter.events.impl

import com.exactpro.th2.lwdataprovider.entities.responses.BaseEventEntity
import com.exactpro.th2.lwdataprovider.filter.DataFilter

class EventSimpleFilter(
    private val values: Collection<String>,
    private val negative: Boolean,
    private val conjunct: Boolean,
    private val accessor: BaseEventEntity.() -> String,
) : DataFilter<BaseEventEntity> {
    override fun match(data: BaseEventEntity): Boolean = data.accessor().let { type ->
        val predicate: (String) -> Boolean = { it.equals(type, ignoreCase = true) }
        values.run { if (conjunct) all(predicate) else any(predicate) }.xor(negative)
    }
}