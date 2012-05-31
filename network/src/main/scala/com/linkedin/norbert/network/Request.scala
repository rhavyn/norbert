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

import java.util.UUID
import com.linkedin.norbert.cluster.{ClusterException, Node}

object Request {
  def apply[RequestMsg, ResponseMsg](message: RequestMsg, node: Node,
                                     inputSerializer: InputSerializer[RequestMsg, ResponseMsg], outputSerializer: OutputSerializer[RequestMsg, ResponseMsg],
                                     callback: Either[Throwable, ResponseMsg] => Unit, retryAttempt: Int = 0): Request[RequestMsg, ResponseMsg] = {
    new Request(message, node, inputSerializer, outputSerializer, callback, retryAttempt)
  }
}

class Request[RequestMsg, ResponseMsg](val message: RequestMsg, val node: Node,
                                       val inputSerializer: InputSerializer[RequestMsg, ResponseMsg], val outputSerializer: OutputSerializer[RequestMsg, ResponseMsg],
                                       val callback: Either[Throwable, ResponseMsg] => Unit, val retryAttempt: Int = 0) {
  val id = UUID.randomUUID
  val timestamp = System.currentTimeMillis

  def name: String = {
    inputSerializer.requestName
  }

  def requestBytes: Array[Byte] = outputSerializer.requestToBytes(message)

  def onFailure(exception: Throwable) {
    callback(Left(exception))
  }

  def onSuccess(bytes: Array[Byte]) {
    callback(try {
      Right(inputSerializer.responseFromBytes(bytes))
    } catch {
      case ex: Exception => Left(new ClusterException("Exception while deserializing response", ex))
    })
  }

  override def toString: String = {
    "[Request: %s, %s, retry=%d]".format(message, node, retryAttempt)
  }

  // TODO: Use the id for overriding equals and hashcode
}

object PartitionedRequest {

  def apply[PartitionedId, RequestMsg, ResponseMsg](message: RequestMsg, node: Node, ids: Set[PartitionedId], requestBuilder: (Node, Set[PartitionedId]) => RequestMsg,
                                                    inputSerializer: InputSerializer[RequestMsg, ResponseMsg], outputSerializer: OutputSerializer[RequestMsg, ResponseMsg],
                                                    callback: Either[Throwable, ResponseMsg] => Unit, retryAttempt: Int = 0,
                                                    responseIterator: Option[ResponseIterator[ResponseMsg]] = None): PartitionedRequest[PartitionedId, RequestMsg, ResponseMsg] = {
    new PartitionedRequest(message, node, ids, requestBuilder, inputSerializer, outputSerializer, callback, retryAttempt, responseIterator)
  }

}

class PartitionedRequest[PartitionedId, RequestMsg, ResponseMsg](override val message: RequestMsg, override val node: Node, val partitionedIds: Set[PartitionedId], val requestBuilder: (Node, Set[PartitionedId]) => RequestMsg,
                                                                 override val inputSerializer: InputSerializer[RequestMsg, ResponseMsg], override val outputSerializer: OutputSerializer[RequestMsg, ResponseMsg],
                                                                 override val callback: Either[Throwable, ResponseMsg] => Unit, override val retryAttempt: Int = 0,
                                                                 val responseIterator: Option[ResponseIterator[ResponseMsg]] = None)
  extends Request[RequestMsg, ResponseMsg](message, node, inputSerializer, outputSerializer, callback, retryAttempt)  {

  override def toString: String = {
    "[PartitionedRequest: %s, %s, ids=%s, retry=%d]".format(message, node, partitionedIds, retryAttempt)
  }
}

/**
 * Provides access to Request Context
 */
trait RequestAccess[Request] {
  def request: Request
}
