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

/**
 * A <code>RouterFactoryComponent</code> implementation that provides a consistent hash routing strategy.
 */
trait ConsistentHashRouterFactoryComponent extends RouterFactoryComponent {
  override val routerFactory: ConsistentHashRouterFactory

  /**
   * A <code>RouterFactory</code> implementation that provides a consistent hash routing strategy. Users must
   * implement the <code>calculateHash</code> method.
   */
  abstract class ConsistentHashRouterFactory(np: Int) extends RouterFactory {
    def newRouter(nodes: Seq[Node]): Router = new Router with ConsistentHashRouterHelper {
      protected val partitionToNodeMap = generatePartitionToNodeMap(nodes, np)

      def apply(id: Id): Option[Node] = nodeForPartition(partitionForId(id))
    }

    /**
     * Calculates the id of the partition on which the specified <code>Id</code> resides.
     *
     * @param id the <code>Id</code> to map to a partition
     *
     * @return the id of the partition on which the <code>Id</code> resides
     */
    def partitionForId(id: Id): Int = calculateHash(id).abs % np

    /**
     * Hashes the <code>Id</code> provided. Users must implement this method. The <code>HashFunctions</code>
     * object provides an implementation of the FNV hash which may help in the implementation.
     *
     * @param id the <code>Id</code> to hash
     *
     * @return the hashed value
     */
    protected def calculateHash(id: Id): Int
  }
}
