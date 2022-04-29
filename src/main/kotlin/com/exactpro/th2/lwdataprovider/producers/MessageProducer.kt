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

package com.exactpro.th2.lwdataprovider.producers

import com.exactpro.th2.lwdataprovider.CustomJsonFormatter
import com.exactpro.th2.lwdataprovider.RequestedMessageDetails
import com.exactpro.th2.lwdataprovider.entities.responses.ProviderMessage
import com.exactpro.th2.lwdataprovider.entities.responses.ProviderParsedMessage
import java.util.Base64
import java.util.Collections
import kotlin.streams.toList

class MessageProducer {

    companion object {

        public fun createMessage(rawMessage: RequestedMessageDetails, formatter: CustomJsonFormatter): ProviderMessage {
            return ProviderMessage(
                rawMessage.storedMessage,
                rawMessage.parsedMessage?.let {
                    it.stream().map { msg ->
                        ProviderParsedMessage(msg.metadata.id, formatter.print(msg))
                    }.toList() 
                } ?: Collections.emptyList<ProviderParsedMessage>(),
                rawMessage.rawMessage.let {
                    Base64.getEncoder().encodeToString(it.body.toByteArray())
                }
            )
        }

        public fun createOnlyRawMessage(rawMessage: RequestedMessageDetails): ProviderMessage {
            return ProviderMessage(
                rawMessage.storedMessage,
                Collections.emptyList<ProviderParsedMessage>(),
                rawMessage.rawMessage.let {
                    Base64.getEncoder().encodeToString(it.body.toByteArray())
                }
            )
        }
    }
    
}