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

import java.io._
import com.google.protobuf.Message

object JavaSerializer {
  def apply[RequestMsg, ResponseMsg]
		(requestClass: Class[RequestMsg], responseClass: Class[ResponseMsg]): JavaSerializer[RequestMsg, ResponseMsg] =
	  apply(
		  requestClass.getName,
	    responseClass.getName,
		  requestClass,
	    responseClass)

	def apply[RequestMsg, ResponseMsg]
	(requestName: String, responseName: String, requestClass: Class[RequestMsg], responseClass: Class[ResponseMsg]): JavaSerializer[RequestMsg, ResponseMsg] =
  new JavaSerializer(
	  requestName,
    responseName,
	  requestClass,
    responseClass)

	def apply[RequestMsg, ResponseMsg]
	(requestName: String, requestClass: Class[RequestMsg], responseClass: Class[ResponseMsg]): JavaSerializer[RequestMsg, ResponseMsg] =
  apply(
	  requestName,
    responseClass.getName,
	  requestClass,
    responseClass)

	def apply[RequestMsg, ResponseMsg]
  (implicit requestManifest: ClassManifest[RequestMsg], responseManifest: ClassManifest[ResponseMsg]): JavaSerializer[RequestMsg, ResponseMsg] =
    apply(requestManifest.erasure.asInstanceOf[Class[RequestMsg]], responseManifest.erasure.asInstanceOf[Class[ResponseMsg]])
}

class JavaSerializer[RequestMsg, ResponseMsg](reqName: String, resName: String,
                                              requestClass: Class[RequestMsg], responseClass: Class[ResponseMsg])
  extends Serializer[RequestMsg, ResponseMsg] {
  def requestName = reqName
  def responseName = resName

  private def toBytes[T](message: T): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(message)
    baos.toByteArray
  }

  private def fromBytes[T](bytes: Array[Byte]): T = {
    val bais = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bais)
    ois.readObject.asInstanceOf[T]
  }

  def requestToBytes(message: RequestMsg): Array[Byte] = toBytes(message)

  def requestFromBytes(bytes: Array[Byte]): RequestMsg = fromBytes(bytes)

  def responseToBytes(message: ResponseMsg): Array[Byte] = toBytes(message)

  def responseFromBytes(bytes: Array[Byte]): ResponseMsg = fromBytes(bytes)
}

object ProtobufSerializer {
  def build[RequestMsg <: Message, ResponseMsg <: Message](requestPrototype: RequestMsg, responsePrototype: ResponseMsg): ProtobufSerializer[RequestMsg, ResponseMsg] =
    new ProtobufSerializer(requestPrototype, responsePrototype)
}

class ProtobufSerializer[RequestMsg <: Message, ResponseMsg <: Message](requestPrototype: RequestMsg, responsePrototype: ResponseMsg) extends Serializer[RequestMsg, ResponseMsg] {
  def requestName = requestPrototype.getDescriptorForType.getFullName

  def responseName = responsePrototype.getDescriptorForType.getFullName

  def requestToBytes(request: RequestMsg) = request.toByteArray

  def responseToBytes(response: ResponseMsg) = response.toByteArray

  def requestFromBytes(bytes: Array[Byte]) = (requestPrototype.newBuilderForType.mergeFrom(bytes).build).asInstanceOf[RequestMsg]

  def responseFromBytes(bytes: Array[Byte]) = (responsePrototype.newBuilderForType.mergeFrom(bytes).build).asInstanceOf[ResponseMsg]
}