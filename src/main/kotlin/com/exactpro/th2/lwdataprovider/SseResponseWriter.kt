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


import com.exactpro.th2.lwdataprovider.http.SseBufferedWriter
import java.io.BufferedWriter
import java.io.Writer
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class SseResponseWriter (private val srcWriter: Writer){
    
    private val writer = BufferedWriter(srcWriter)
    
    private val lock = ReentrantLock()

    fun writeEvent(event: SseEvent) {
        eventWrite(event)
    }

    fun closeWriter() {
        lock.withLock { 
            this.writer.flush()
            this.writer.close()
        }
    }

    private fun eventWrite(event: SseEvent) {
        lock.withLock {
            with(writer) {
                writer.write("event: ", event.event.typeName, "\n")


                write("data: ", event.data, "\n")

                if (event.metadata != null) {
                    write("id: ", event.metadata, "\n")
                }

                write("\n")
//                this.writer.finishMessage()
            }
        }
    }

    private fun BufferedWriter.write(vararg strings: String) {
        strings.forEach(this::write)
    }
}