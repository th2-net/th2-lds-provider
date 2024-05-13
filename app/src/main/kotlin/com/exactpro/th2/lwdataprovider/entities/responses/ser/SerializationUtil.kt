/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.lwdataprovider.entities.responses.ser

import kotlin.math.ceil
import kotlin.math.log10
import kotlin.math.max

internal fun numberOfDigits(value: Int): Int {
    val log10 = log10(value.toDouble())
    val ceilLog10 = max(ceil(log10).toInt(), 1)
    val correction = if (value > 0
        && value % 10 == 0 // values like 1000 should have 4 digits but log10 will return 3
        && log10.toInt() == ceilLog10 // values like 1010 match the previous condition, but we should not add 1 in that case
    ) {
        1
    } else {
        0
    }
    return ceilLog10 + correction
}