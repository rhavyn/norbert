package com.linkedin.norbert.network

import java.io.{OutputStream, InputStream}

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

/**
 * When you don't care about variance. IE Java users
 */
trait Serializer[RequestMsg, ResponseMsg] extends InputSerializer[RequestMsg, ResponseMsg] with OutputSerializer[RequestMsg, ResponseMsg]


// Split up for correct variance
trait OutputSerializer[-RequestMsg, -ResponseMsg] {
  def requestToBytes(request: RequestMsg): Array[Byte]
  def responseToBytes(response: ResponseMsg): Array[Byte]
}

trait InputSerializer[+RequestMsg, +ResponseMsg] {
  def nameOfRequestMessage: String

  def requestFromBytes(bytes: Array[Byte]): RequestMsg
  def responseFromBytes(bytes: Array[Byte]): ResponseMsg
}

//  def writeSerializedRequestMessage(message: RequestMsg, output: OutputStream): Unit
//  def readSerializedResponseMessage(input: InputStream): ResponseMsg
