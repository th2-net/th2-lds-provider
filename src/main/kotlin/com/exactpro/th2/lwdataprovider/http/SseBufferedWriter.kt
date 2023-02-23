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

package com.exactpro.th2.lwdataprovider.http

import java.io.Writer

class SseBufferedWriter(private val srcWriter: Writer) {

    private val buffer: StringBuilder = StringBuilder()
    
    
    fun close() {
        flush()
        srcWriter.close()
    }

    fun flush() {
        if (buffer.isNotEmpty()) {
            finishMessage()
        }
        srcWriter.flush()
    }

    fun finishMessage() {
        this.srcWriter.write(buffer.toString())
        this.buffer.setLength(0)
        this.srcWriter.flush()
    }

    fun write(char: Char) {
        buffer.append(char)
    }

    fun write(str: String) {
        buffer.append(str)
    }

    fun write(vararg str: String) {
        str.forEach { buffer.append(it) }
    }
    
}