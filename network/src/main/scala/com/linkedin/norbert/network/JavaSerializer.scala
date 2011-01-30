package com.linkedin.norbert.network

import java.io._

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

object JavaSerializer {
  def build[RequestMsg, ResponseMsg]
  (implicit requestManifest: ClassManifest[RequestMsg], responseManifest: ClassManifest[ResponseMsg]): JavaSerializer[RequestMsg, ResponseMsg] =
    new JavaSerializer[RequestMsg, ResponseMsg](requestManifest.erasure.asInstanceOf[Class[RequestMsg]],
                                                responseManifest.erasure.asInstanceOf[Class[ResponseMsg]])
}


class JavaSerializer[RequestMsg, ResponseMsg](requestClass: Class[RequestMsg], responseClass: Class[ResponseMsg])
  extends Serializer[RequestMsg, ResponseMsg] {
  def nameOfRequestMessage = requestClass.getName + " -> " + responseClass.getName

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

  def requestToBytes(message: RequestMsg) = toBytes(message)

  def requestFromBytes(bytes: Array[Byte]) = fromBytes(bytes)

  def responseToBytes(message: ResponseMsg) = toBytes(message)

  def responseFromBytes(bytes: Array[Byte]) = fromBytes(bytes)
}