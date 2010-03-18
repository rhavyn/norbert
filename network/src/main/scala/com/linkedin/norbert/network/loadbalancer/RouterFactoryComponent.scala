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
package com.linkedin.norbert.network.loadbalancer

import com.linkedin.norbert.cluster.{Node, InvalidClusterException}

/**
 * A component which provides the machinery for the <code>ClusterClient</code> to route a request to the
 * <code>Node</code> which can process the request.
 */
trait RouterFactoryComponent {
  /**
   * The type of the id which the <code>Router</code> can process.
   */
  type Id

  val routerFactory: RouterFactory

  /**
   * A <code>Router</code> provides a mapping between an <code>Id</code> and a <code>Node</code> which
   * can process requests for the <code>Id</code>.
   */
  trait Router extends Function1[Id, Option[Node]]

  /**
   * A factory which can generate <code>Router</code>s.
   */
  trait RouterFactory {
    /**
     * Create a new router instance based on the currently available <code>Node</code>s.
     *
     * @param nodes the currently available <code>Node</code>s in the cluster
     *
     * @return a new <code>Router</code> instance
     * @throws InvalidClusterException thrown to indicate that the current cluster topology is invalid in some way
     */
    @throws(classOf[InvalidClusterException])
    def newRouter(nodes: Seq[Node]): Router
  }
}
