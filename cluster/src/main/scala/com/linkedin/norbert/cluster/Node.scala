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
package com.linkedin.norbert.cluster

import java.net.InetSocketAddress
import scala.reflect.BeanProperty
import com.google.protobuf.InvalidProtocolBufferException
import com.linkedin.norbert.protos.NorbertProtos

/**
 * The <code>Node</code> companion object. Provides factory methods and implicits.
 */
object Node {
  /**
   * Create a <code>Node</code> instance.
   *
   * @param id the id of the node
   * @param hostname the name of the host the node runs on
   * @param port the port on the host to which requests should be sent
   * @param partitions the partitions for which the node can handle requests
   * @param available whether or not the node is currently able to process requests
   *
   * @return a new <code>Node</code> instance
   */
  def apply(id: Int, hostname: String, port: Int, partitions: Array[Int], available: Boolean): Node = Node(id, new InetSocketAddress(hostname, port), partitions, available)

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
    import collection.jcl.Conversions._

    try {
      val node = NorbertProtos.Node.newBuilder.mergeFrom(bytes).build
      val partitions = new Array[Int](node.getPartitionsCount)
      node.getPartitionsList.asInstanceOf[java.util.List[Int]].copyToArray(partitions, 0)

      Node(node.getId, new InetSocketAddress(node.getHostname, node.getPort), partitions, available)
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
    builder.setId(node.id).setHostname(node.address.getHostName).setPort(node.address.getPort)
    node.partitions.foreach(builder.addPartitions(_))

    builder.build.toByteArray
  }
}

/**
 * A representation of a physical node in the cluster.
 *
 * @param id the id of the node
 * @param address the <code>InetSocketAddress</code> on which requests can be sent to the node
 * @param partitions the partitions for which the node can handle requests
 * @param available whether or not the node is currently able to process requests
 */
final case class Node(@BeanProperty id: Int, @BeanProperty address: InetSocketAddress,
        @BeanProperty partitions: Array[Int], @BeanProperty available: Boolean) {
  override def hashCode = id.hashCode

  override def equals(other: Any) = other match {
    case that: Node => this.id == that.id && this.address == that.address
    case _ => false
  }

  override def toString = "Node(%d,%s,[%s],%b)".format(id, address, partitions.mkString(","), available)
}
