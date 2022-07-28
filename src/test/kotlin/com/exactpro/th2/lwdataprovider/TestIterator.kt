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

import com.exactpro.th2.common.grpc.ConnectionID
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.MessageMetadata
import com.exactpro.th2.lwdataprovider.workers.CodecMessageListener
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

class TestIterator {
    
    private fun getMessage(alias: String, dir: Direction, index:Long, subIndex : Int? = null ) : Message {
        return Message.newBuilder().setMetadata(
            MessageMetadata.newBuilder().setId(MessageID.newBuilder().apply {
                setConnectionId(ConnectionID.newBuilder().setSessionAlias(alias))
                direction = dir
                sequence = index
                subIndex?.let { addSubsequence(it) }
            }
               )
        ).build()
    }
    
    @Test
    fun testIterator () {
        val msg1 = getMessage("str123", Direction.FIRST, 12345L)
        val arrayList = listOf(msg1)
        val arrayList2 = listOf(listOf(msg1))

        val messageIterator = CodecMessageListener.MessageIterator(arrayList.iterator()).asSequence().toList()

        Assertions.assertEquals(arrayList2, messageIterator)
    }

    @Test
    fun testIterator2 () {
        val msg1 = getMessage("str123", Direction.FIRST, 12345L, 1)
        val msg2 = getMessage("str123", Direction.FIRST, 12345L, 2)
        val arrayList = listOf(msg1, msg2)
        val arrayList2 = listOf(listOf(msg1, msg2))

        val messageIterator = CodecMessageListener.MessageIterator(arrayList.iterator()).asSequence().toList()

        Assertions.assertEquals(arrayList2, messageIterator)
    }

    @Test
    fun testIterator3 () {
        val msg1 = getMessage("str123", Direction.FIRST, 12345L)
        val msg2 = getMessage("str123", Direction.FIRST, 12346L)
        val arrayList = listOf(msg1, msg2)
        val arrayList2 = listOf(listOf(msg1), listOf(msg2))

        val messageIterator = CodecMessageListener.MessageIterator(arrayList.iterator()).asSequence().toList()

        Assertions.assertEquals(arrayList2, messageIterator)
    }

    @Test
    fun testIterator4 () {
        val msg1 = getMessage("str123", Direction.FIRST, 12345L)
        val msg2 = getMessage("str123", Direction.FIRST, 12346L)
        val msg3 = getMessage("str123", Direction.FIRST, 12346L)
        val arrayList = listOf(msg1, msg2, msg3)
        val arrayList2 = listOf(listOf(msg1), listOf(msg2, msg3))

        val messageIterator = CodecMessageListener.MessageIterator(arrayList.iterator()).asSequence().toList()

        Assertions.assertEquals(arrayList2, messageIterator)
    }

    @Test
    fun testIterator5 () {
        val msg1 = getMessage("str123", Direction.FIRST, 12345L)
        val msg2 = getMessage("str123", Direction.FIRST, 12346L, 1)
        val msg3 = getMessage("str123", Direction.FIRST, 12346L, 2)
        val arrayList = listOf(msg1, msg2, msg3)
        val arrayList2 = listOf(listOf(msg1), listOf(msg2, msg3))

        val messageIterator = CodecMessageListener.MessageIterator(arrayList.iterator()).asSequence().toList()

        Assertions.assertEquals(arrayList2, messageIterator)
    }


}