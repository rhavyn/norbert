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

case class Request[RequestMsg, ResponseMsg](message: RequestMsg, node: Node,
                                            inputSerializer: InputSerializer[RequestMsg, ResponseMsg], outputSerializer: OutputSerializer[RequestMsg, ResponseMsg],
                                            callback: Either[Throwable, ResponseMsg] => Unit) {
  val id = UUID.randomUUID
  val timestamp = System.currentTimeMillis

  def name: String = {
    inputSerializer.nameOfRequestMessage
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

  // TODO: Use the id for overriding equals and hashcode
}