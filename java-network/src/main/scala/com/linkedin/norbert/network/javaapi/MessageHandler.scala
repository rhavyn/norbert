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
package com.linkedin.norbert.network.javaapi

import com.google.protobuf.Message

/**
 * A <code>MessageHandler</code> is responsible for processing incoming <code>Messages</code>.
 */
trait MessageHandler {

  /**
   * Returns an array of the <code>Messages</code> which this handler is capable of processing.
   *
   * @return an array of the <code>Messages</code> which this handler is capable of processing
   */
  def getMessages: Array[Message]
  
  /**
   * Processes an incoming <code>Message</code>. A null return value indicates that there is nothing to
   * return to the client. A returned <code>Message</code> will be sent to the client as the response.
   *
   * @param message the incoming <code>Message</code>
   *
   * @return if the handler wants to send a response to the client, the message to send. Otherwise, null.
   */
  @throws(classOf[Exception])
  def handleMessage(message: Message): Message
}
