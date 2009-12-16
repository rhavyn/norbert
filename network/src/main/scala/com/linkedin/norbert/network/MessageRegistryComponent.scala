/*
 * Copyright 2009 LinkedIn, Inc
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
package com.linkedin.norbert.network

import com.google.protobuf.Message

trait MessageRegistryComponent {
  val messageRegistry: MessageRegistry

  trait MessageRegistry {
    def defaultInstanceForClassName(className: String): Option[Message]
    def defaultInstanceAndHandlerForClassName(className: String): Option[(Message, (Message) => Option[Message])]
  }

  class DefaultMessageRegistry[T <: Message](responseMessages: Array[T], requestMessages: Array[(T, (Message) => Option[Message])]) extends MessageRegistry {
    def this(responseMessages: Array[T]) = this (responseMessages, Array[(T, (Message) => Option[Message])]())
    def this(requestMessages: Array[(T, (Message) => Option[Message])]) = this(Array[T](), requestMessages)
    
    private val handledMessageMap = (if (requestMessages == null) {
      Array[(Message, (Message) => Option[Message])]()
    } else {
      requestMessages
    }).foldLeft(Map.empty[String, (Message, Message => Option[Message])]) { case (map, (message, handler)) =>
      map + (message.getClass.getName -> (message.getDefaultInstanceForType, handler))
    }

    private val unhandledMessageMap = (if (responseMessages == null) {
      Array[Message]()
    } else {
      responseMessages
    }).foldLeft(Map.empty[String, Message]) { case (map, message) =>
      map + (message.getClass.getName -> message.getDefaultInstanceForType)
    }

    def defaultInstanceAndHandlerForClassName(className: String) = handledMessageMap.get(className)

    def defaultInstanceForClassName(className: String) = unhandledMessageMap.get(className).
            orElse(handledMessageMap.get(className) map { case (message, handler) => message })
  }
}
