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
package com.linkedin.norbert.javacompat.network;

import java.util.Set;

import com.linkedin.norbert.javacompat.cluster.Node;
import com.linkedin.norbert.network.ResponseIterator;

/**
 * A <code>ScatterGatherHandler</code> is used to customize an outgoing <code>Message</code> and to
 * aggregate the incoming responses.
 */
public interface ScatterGatherHandler<RequestMsg, ResponseMsg, T, PartitionedId> {
  /**
   * This method is called after all messages are sent and allows the user to aggregate the responses.
   *
   * @param responseIterator the <code>ResponseIterator</code> to retrieve responses
   *
   * @return A user defined value.  This value is passed on to the client.
   * @throws Exception any exception thrown will be passed on to the client
   */
  T gatherResponses(ResponseIterator<ResponseMsg> responseIterator) throws Exception;
}
