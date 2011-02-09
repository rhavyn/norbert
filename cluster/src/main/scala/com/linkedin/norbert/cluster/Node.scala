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
package cluster

import com.google.protobuf.InvalidProtocolBufferException
import protos.NorbertProtos

/**
 * The <code>Node</code> companion object. Provides factory methods and implicits.
 */
object Node {

  /**
   * Creates a <code>Node</code> instance using the serialized state of a node.
   *
   * @param id the id of the node
   * @param bytes the serialized state of the a node.  <code>Node</code>s should be serialized using the
   * <code>nodeToByteArray</code> implicit implemented in this object.
   * @param available whether or not the node is currently able to process requests
   *
   * @return a new <code>Node</code> instance
   */
  def apply(id: Int, bytes: Array[Byte], available: Boolean): Node = {
    import collection.JavaConversions._

    try {
      val node = NorbertProtos.Node.newBuilder.mergeFrom(bytes).build
      val partitions = node.getPartitionList.asInstanceOf[java.util.List[Int]].foldLeft(Set[Int]()) { (set, i) => set + i }

      Node(node.getId, node.getUrl, available, partitions)
    } catch {
      case ex: InvalidProtocolBufferException => throw new InvalidNodeException("Error deserializing node", ex)
    }
  }

  /**
   * Implicit method which serializes a <code>Node</code> instance into an array of bytes.
   *
   * @param node the <code>Node</code> to serialize
   *
   * @return the serialized <code>Node</code>
   */
  implicit def nodeToByteArray(node: Node): Array[Byte] = {
    val builder = NorbertProtos.Node.newBuilder
    builder.setId(node.id).setUrl(node.url)
    node.partitionIds.foreach(builder.addPartition(_))

    builder.build.toByteArray
  }
}

/**
 * A representation of a physical node in the cluster.
 *
 * @param id the id of the node
 * @param address the url to which requests can be sent to the node
 * @param available whether or not the node is currently able to process requests
 * @param partitions the partitions for which the node can handle requests
 */
final case class Node(id: Int, url: String, available: Boolean, partitionIds: Set[Int] = Set.empty) {
  if (url == null) throw new NullPointerException("url must not be null")
  if (partitionIds == null) throw new NullPointerException("partitions must not be null")

  override def hashCode = id.hashCode

  override def equals(other: Any) = other match {
    case that: Node => this.id == that.id && this.url == that.url
    case _ => false
  }

  override def toString = "Node(%d,%s,[%s],%b)".format(id, url, partitionIds.mkString(","), available)
  var health: Int = 10
  def getHealthScore = health;
  def setHealth(h: Int) { health = h }
}
