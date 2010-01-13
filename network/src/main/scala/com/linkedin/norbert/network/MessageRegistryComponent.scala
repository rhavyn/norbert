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
package com.linkedin.norbert.network

import com.google.protobuf.Message
import java.lang.String

/**
 * A component which provides a registry for the messages sent to a server or received by a client.
 */
trait MessageRegistryComponent {
  val messageRegistry: MessageRegistry

  /**
   * A <code>MessageRegistry</code> provides the default instances of the <code>Message</code>s a client
   * receives as well as the pairs of <code>Message</code>s and handlers a server uses to process requests. 
   */
  trait MessageRegistry {
    def defaultInstanceForClassName(className: String): Option[Message]
    def handlerForClassName(className: String): Option[(Message) => Option[Message]]
    def defaultInstanceAndHandlerForClassName(className: String): Option[(Message, (Message) => Option[Message])]
  }

  /**
   * A <code>MessageRegistry</code> implementation that provides a registry of <code>Message</code>s received
   * by a client and/or the pairs of <code>Message</code>s processed by a server and a handler method to
   * process the <code>Message</code>
   *
   * @param responseMessages an array of the <code>Message</code>s received by a client
   * @param requestMessages an array of the pairs of <code>Message</code>s processed by a server and a handler
   * method to process <code>Message</code>. The handler method is passed the recieved <code>Message</code> and should
   * return an <code>Option</code> where None means there is no response to the client and a Some represents
   * there is a response with the value of the Some being the <code>Message</code> to return.
   */
  class DefaultMessageRegistry[T <: Message](responseMessages: Array[T], requestMessages: Array[(T, (Message) => Option[Message])]) extends MessageRegistry {
    /**
     * A constructor for clients that only receive responses to sent messages.
     *
     * @param responseMessages an array of the <code>Message</code>s received by a client
     */
    def this(responseMessages: Array[T]) = this (responseMessages, Array[(T, (Message) => Option[Message])]())

    /**
     * A constructor for serves that only receive requests from clients.
     *
     * @param requestMessages an array of the pairs of <code>Message</code>s processed by a server and a handler
     * method to process <code>Message</code>. The handler method is passed the recieved <code>Message</code> and should
     * return an <code>Option</code> where None means there is no response to the client and a Some represents
     * there is a response with the value of the Some being the <code>Message</code> to return.
     */
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

    def handlerForClassName(className: String) = handledMessageMap.get(className) map { case (message, handler) => handler }

    def defaultInstanceForClassName(className: String) = unhandledMessageMap.get(className).
            orElse(handledMessageMap.get(className) map { case (message, handler) => message })
  }
}
