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
import mu.KotlinLogging
import org.apache.commons.lang3.StringUtils
import java.io.Writer
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class SseResponseWriter (srcWriter: Writer){
    
    private val writer: SseBufferedWriter = SseBufferedWriter(srcWriter)
    
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
            this.writer.write("event: ", event.event.typeName, "\n")

            val data = event.data.get()
            this.writer.write("data: ", data, "\n")

            LOGGER.trace { "Writing (${event.metadata}) data: ${StringUtils.abbreviate(data, 100)}" }
            
            if (event.metadata != null) {
                this.writer.write("id: ", event.metadata, "\n")
            }

            this.writer.write('\n')
            this.writer.finishMessage()
        }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }
}