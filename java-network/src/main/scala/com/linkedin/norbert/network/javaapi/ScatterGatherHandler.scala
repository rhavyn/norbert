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
import com.linkedin.norbert.cluster.Node

/**
 * A <code>ScatterGatherHandler</code> is used to customize an outgoing <code>Message</code> and to
 * aggregate the incoming responses.
 */
trait ScatterGatherHandler[A] {

  /**
   * This method is called immediately before the <code>Message</code> is sent and allows the user to
   * customize it for the particular <code>Node</code> and ids the message is being sent to.  The
   * returned <code>Message</code> is the one that will actually be sent.
   *
   * @param originalRequest the <code>Message</code> to be customized
   * @param node the <code>Node</code> the request is being sent to
   * @param ids the ids that are on the node the request is being sent to
   *
   * @throws Exception any exception thrown will be passed on to the client
   */
  @throws(classOf[Exception])
  def customizeMessage(originalRequest: Message, node: Node, ids: Array[Int]): Message

  /**
   * This method is called after all messages are sent and allows the user to aggregate the responses.
   *
   * @param originalRequest the original (uncustomized) request that was sent
   * @param responseIterator the <code>ResponseIterator</code> to retrieve responses
   *
   * @returns A user defined value.  This value is passed on to the client.
   * @throws Exception any exception thrown will be passed on to the client
   */
  @throws(classOf[Exception])
  def gatherResponses(originalRequest: Message, responseIterator: ResponseIterator): A
}
