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
package com.linkedin.norbert

import javacompat.cluster.{JavaNode, Node => JNode}
import com.linkedin.norbert.cluster.{Node => SNode}

package object javacompat {
  implicit def scalaSetToJavaSet[T](set: Set[T]): java.util.Set[T] = {
    val s = new java.util.HashSet[T]
    set.foreach { elem => s.add(elem) }
    s
  }

  implicit def javaSetToImmutableSet[T](nodes: java.util.Set[T]): Set[T] = {
    collection.JavaConversions.asSet(nodes).foldLeft(Set[T]()) { (set, n) => set + n }
  }

  implicit def scalaNodeToJavaNode(node: SNode): JNode = {
    if (node == null) null else JavaNode(node)
  }

  implicit def javaNodeToScalaNode(node: JNode): SNode = {
    if (node == null) null else SNode(node.getId, node.getUrl, node.isAvailable,
      node.getPartitionIds.asInstanceOf[java.util.Set[Int]].foldLeft(Set[Int]()) { (set, id) => set + id })
  }

  implicit def convertNodeSet(set: Set[SNode]): java.util.Set[JNode] =
    set.map(node => scalaNodeToJavaNode(node))
}
