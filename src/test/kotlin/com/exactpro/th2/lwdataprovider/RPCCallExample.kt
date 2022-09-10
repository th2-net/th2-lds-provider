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
package com.exactpro.th2.lwdataprovider

import com.exactpro.th2.dataprovider.grpc.DataProviderGrpc
import com.exactpro.th2.dataprovider.grpc.MessageGroupsSearchRequest
import io.grpc.ManagedChannelBuilder
import java.time.Instant

class RPCCallExample {

    companion object {

        @JvmStatic
        fun main(args: Array<String>) {
            val channel = ManagedChannelBuilder.forAddress(args[0], args[1].toInt()).usePlaintext().build()
            val dataProvider = DataProviderGrpc.newBlockingStub(channel)
//            val context = Context.current()
//                .withCancellation()
            try {
                val count = dataProvider.searchMessageGroups(MessageGroupsSearchRequest.newBuilder().apply {
                    startTimestampBuilder.apply {
                        Instant.parse(args[2]).also {
                            seconds = it.epochSecond
                            nanos = it.nano
                        }
                    }
                    endTimestampBuilder.apply {
                        Instant.parse(args[3]).also {
                            seconds = it.epochSecond
                            nanos = it.nano
                        }
                    }
                    addMessageGroup(MessageGroupsSearchRequest.Group.newBuilder().apply {
                        name = args[4]
                    }.build())
                }.build()).asSequence().count()

                println(count)
            } finally {
//                context.cancel(null)
            }
        }
    }
}