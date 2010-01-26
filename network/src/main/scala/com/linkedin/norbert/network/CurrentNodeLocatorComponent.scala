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
 * A component which provides a way to look up the current node.
 */
trait CurrentNodeLocatorComponent {
  def currentNodeLocator: CurrentNodeLocator

  /**
   * The <code>CurrentNodeLocator</code> provides a way to look up the node for which the application
   * services requests.
   */
  trait CurrentNodeLocator {
    /**
     * Looks up the node for which the application services requests.
     *
     * @returns The node for which the application services requests, or null if the application is
     * not currently servicing requests, or if the application is only a client of the cluster.
     */
    def currentNode: Node
  }
}

/**
 * Client side specific implementation of the <code>CurrentNodeLocatorComponent</code>.
 */
trait ClientCurrentNodeLocatorComponent extends CurrentNodeLocatorComponent {
  class ClientCurrentNodeLocator extends CurrentNodeLocator {
    def currentNode: Node = null
  }
}

/**
 * Server side specific implementation of the <code>CurrentNodeLocatorComponent</code>.
 */
trait ServerCurrentNodeLocatorComponent extends CurrentNodeLocatorComponent {
  this: NetworkServerComponent =>

  class ServerCurrentNodeLocator extends CurrentNodeLocator {
    def currentNode: Node = networkServer.currentNode
  }
}
