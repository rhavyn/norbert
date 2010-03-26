/*
 * Copyright 2009-2010 LinkedIn, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.linkedin.norbert.network.common

import com.google.protobuf.Message
import java.lang.NullPointerException
import com.linkedin.norbert.network.InvalidMessageException

trait MessageRegistryComponent {
  val messageRegistry: MessageRegistry
}

class MessageRegistry {
  private var messageMap = Map[String, (Message, Message)]()

  def contains(requestMessage: Message): Boolean = messageMap.contains(requestMessage.getDescriptorForType.getFullName)

  def hasResponse(requestMessage: Message): Boolean = getMessagePair(requestMessage)._2 != null

  def registerMessage(requestMessage: Message, responseMessage: Message) {
    if (requestMessage == null) throw new NullPointerException
    val response = if (responseMessage == null) null else responseMessage.getDefaultInstanceForType

    messageMap += (requestMessage.getDescriptorForType.getFullName -> (requestMessage.getDefaultInstanceForType, response))
  }

  def responseMessageDefaultInstanceFor(requestMessage: Message): Message = getMessagePair(requestMessage)._2

  private def getMessagePair(requestMessage: Message) = {
    val name = requestMessage.getDescriptorForType.getFullName
    messageMap.get(name).getOrElse(throw new InvalidMessageException("No such message of type %s registered".format(name)))
  }
}

