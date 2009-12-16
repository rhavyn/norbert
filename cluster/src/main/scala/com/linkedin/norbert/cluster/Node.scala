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
package com.linkedin.norbert.cluster

import java.net.InetSocketAddress
import scala.reflect.BeanProperty
import com.google.protobuf.InvalidProtocolBufferException
import com.linkedin.norbert.protos.NorbertProtos

object Node {
  def apply(id: Int, host: String, port: Int, partitions: Array[Int], available: Boolean): Node = Node(id, new InetSocketAddress(host, port), partitions, available)
  
  def apply(id: Int, bytes: Array[Byte], available: Boolean): Node = {
    import collection.jcl.Conversions._

    try {
      val node = NorbertProtos.Node.newBuilder.mergeFrom(bytes).build
      val partitions = new Array[Int](node.getPartitionsCount)
      node.getPartitionsList.asInstanceOf[java.util.List[Int]].copyToArray(partitions, 0)

      Node(node.getId, new InetSocketAddress(node.getHostname, node.getPort), partitions, available)
    }
    catch {
      case ex: InvalidProtocolBufferException => throw new InvalidNodeException("Error deserializing node", ex)
    }
  }

  implicit def nodeToByteArray(node: Node): Array[Byte] = {
    val builder = NorbertProtos.Node.newBuilder
    builder.setId(node.id).setHostname(node.address.getHostName).setPort(node.address.getPort)
    node.partitions.foreach(builder.addPartitions(_))

    builder.build.toByteArray
  }
}

final case class Node(@BeanProperty id: Int, @BeanProperty address: InetSocketAddress,
        @BeanProperty partitions: Array[Int], @BeanProperty available: Boolean) {
  override def hashCode = id.hashCode

  override def equals(other: Any) = other match {
    case that: Node => this.id == that.id && this.address == that.address
    case _ => false
  }

  override def toString = "Node(%d,%s,[%s],%b)".format(id, address, partitions.mkString(","), available)
}
