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
package com.linkedin.norbert.cluster.router

import com.linkedin.norbert.cluster.{Node, RouterFactoryComponent}

trait ConsistentHashRouterFactoryComponent extends RouterFactoryComponent {
  override val routerFactory: ConsistentHashRouterFactory
  
  abstract class ConsistentHashRouterFactory(np: Int) extends RouterFactory with ConsistentHashRouterFactoryHelper {
    def newRouter(nodes: Seq[Node]): Router = new Router with ConsistentHashRouterHelper {
      protected val partitionToNodeMap = generatePartitionToNodeMap(nodes, np)

      def apply(id: Id): Option[Node] = nodeForPartition(partitionForId(id))
    }

    def partitionForId(id: Id): Int = calculateHash(id).abs % np

    protected def calculateHash(id: Id): Int
  }
}
