/*******************************************************************************
 * Copyright 2021-2023 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.lwdataprovider.RequestedMessage
import com.exactpro.th2.lwdataprovider.RequestedMessageDetails
import com.exactpro.th2.lwdataprovider.transport.toProtoMessageId
import com.exactpro.th2.lwdataprovider.entities.responses.ProviderMessage
import com.exactpro.th2.lwdataprovider.entities.responses.ProviderParsedMessage
import java.util.Base64

class MessageProducer {

    companion object {

        fun createMessage(rawMessage: RequestedMessage, formatter: JsonFormatter?, includeRaw: Boolean): ProviderMessage {
            return ProviderMessage(
                rawMessage.storedMessage,
                rawMessage.protoMessage?.takeIf { formatter != null }?.asSequence()?.map { msg ->
                    ProviderParsedMessage(msg.metadata.id, formatter!!.print(msg))
                }?.toList() ?: rawMessage.transportMessage?.takeIf { formatter != null }?.asSequence()?.map { msg ->
                    val book = rawMessage.requestId.bookName
                    val sessionGroup = rawMessage.sessionGroup
                    ProviderParsedMessage(msg.id.toProtoMessageId(book, sessionGroup), formatter!!.print(sessionGroup, msg))
                }?.toList() ?: emptyList(),
                rawMessage.storedMessage.takeIf { includeRaw }?.let {
                    Base64.getEncoder().encodeToString(it.content)
                }
            )
        }
    }
}