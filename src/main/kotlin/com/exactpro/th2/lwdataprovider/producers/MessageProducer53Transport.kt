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

package com.exactpro.th2.lwdataprovider.producers

import com.exactpro.th2.lwdataprovider.RequestedMessage
import com.exactpro.th2.lwdataprovider.entities.responses.ProviderMessage53Transport
import java.util.*

@Deprecated("for 5.3 messages")
class MessageProducer53Transport {

    companion object {
        fun createMessage(
            rawMessage: RequestedMessage,
            formatter: JsonFormatter?,
            includeRaw: Boolean,
        ): ProviderMessage53Transport {
            return ProviderMessage53Transport(
                rawMessage.storedMessage, rawMessage.sessionGroup,
                if (formatter != null) requireNotNull(rawMessage.transportMessage) else null, // FIXME: return only first message instead of merge
                if (includeRaw) rawMessage.storedMessage.let { Base64.getEncoder().encodeToString(it.content) } else null,
            )
        }
    }
}