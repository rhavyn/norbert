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

import com.linkedin.norbert.cluster.Node

/**
 * A component which provides a network server.
 */
trait NetworkServerComponent {
  val networkServer: NetworkServer

  /**
   * A <code>NetworkServer</code> listens for incoming messages and processes them using the handler
   * registered for that message type with the <code>MessageRegistry</code>.
   */
  trait NetworkServer {

    /**
     * Binds the network server to a port and marks the node associated with this server available.
     */
    def bind: Unit

    /**
     * Binds the network server to a port and, if markAvailable is true, marks the node associated with this
     * server available.
     *
     * @param markAvailable specified whether or not to mark the node associated with this server available after
     * binding
     */
    def bind(markAvailable: Boolean): Unit

    /**
     * Retrieves the node associated with this server.
     */
    def currentNode: Node

    /**
     * Marks the node associated with this server available.  If you call <code>bind(false)</code> you must, eventually,
     * call this method before the cluster will start sending this server requests.
     */
    def markAvailable: Unit

    /**
     * Shuts down the <code>NetworkServer</code>.  The server will disconnect from the cluster, unbind from the port,
     * wait for all currently processing requests to finish, close the sockets that have been accepted and any opened
     * client sockets.
     */
    def shutdown: Unit
  }
}
