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
package com.linkedin.norbert.network.router

import com.linkedin.norbert.network.CurrentNodeLocatorComponent
import com.linkedin.norbert.cluster.Node
import com.linkedin.norbert.cluster.router.{ConsistentHashRouterHelper, ConsistentHashRouterFactoryComponent}

trait CurrentNodeAwareConsistentHashRouterFactoryComponent extends ConsistentHashRouterFactoryComponent {
  this: CurrentNodeLocatorComponent =>

  abstract class CurrentNodeAwareConsistentHashRouterFactory(np: Int) extends ConsistentHashRouterFactory(np) {
    override def newRouter(nodes: Seq[Node]): Router = new Router with ConsistentHashRouterHelper {
      protected val partitionToNodeMap = generatePartitionToNodeMap(nodes, np)
      private val currentNodePartitions = nodes.filter(currentNodeLocator.currentNode == _).firstOption match {
        case Some(n) => Set(n.partitions: _*)
        case None => Set.empty[Int]
      }

      def apply(id: Id): Option[Node] = {
        val partitionId = partitionForId(id)
        if (currentNodePartitions.contains(partitionId)) {
          Some(currentNodeLocator.currentNode)
        } else {
          nodeForPartition(partitionId)
        }
      }
    }
  }
}
