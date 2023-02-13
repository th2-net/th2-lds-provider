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

package com.exactpro.th2.lwdataprovider.configuration

import mu.KotlinLogging
import kotlin.reflect.KProperty0

class VariableBuilder {
    
    companion object {
        private val logger = KotlinLogging.logger { }
        
        private fun <T> printToLog(name: String, value: T, isDefault: Boolean, showInLog: Boolean,
                                   converter : (T) -> String = { it.toString() }) {
            logger.info {
                val valueToLog = if (showInLog) converter.invoke(value) else "*****"

                if (isDefault)
                    "property '$name' is not set - defaulting to '$valueToLog'"
                else
                    "property '$name' is set to '$valueToLog'"
            }
        }

        fun <T> getVariable(prop: KProperty0<T?>, defaultValue: T, showInLog: Boolean = true): T {
            return getVariable(prop, defaultValue, showInLog) { it }
        }

        fun <T, R> getVariable(prop: KProperty0<T?>, defaultValue: R, showInLog: Boolean = true, convert: (T) -> R): R {
            val param = prop.get()?.let(convert)
            val value = param ?: defaultValue
            printToLog(prop.name, value as Any, param == null, showInLog)
            return value
        }
    }
}