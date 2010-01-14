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

import com.linkedin.norbert.protos.NorbertProtos

trait MessageHandlerComponent {
  this: MessageRegistryComponent =>

  val messageHandler: MessageHandler

  class MessageHandler {
    def handleMessage(norbertMessage: NorbertProtos.NorbertMessage): Option[NorbertProtos.NorbertMessage] = {
      def newBuilder = {
        val builder = NorbertProtos.NorbertMessage.newBuilder()
        builder.setRequestIdMsb(norbertMessage.getRequestIdMsb)
        builder.setRequestIdLsb(norbertMessage.getRequestIdLsb)
        builder
      }

      def newErrorMessage(ex: Exception) = Some(newBuilder.setMessageName(ex.getClass.getName)
              .setStatus(NorbertProtos.NorbertMessage.Status.ERROR).setErrorMessage(if (ex.getMessage != null) ex.getMessage else "").build)

      if (norbertMessage.getStatus != NorbertProtos.NorbertMessage.Status.OK) {
        newErrorMessage(new InvalidMessageException("Recieved a request in the error state"))
      } else {
        messageRegistry.defaultInstanceAndHandlerForClassName(norbertMessage.getMessageName) match {
          case Some((defaultInstance, handler)) =>
            try {
              handler(defaultInstance.newBuilderForType.mergeFrom(norbertMessage.getMessage).build) map { response =>
                val builder = newBuilder
                builder.setMessageName(response.getClass.getName)
                builder.setMessage(response.toByteString)
                builder.build
              }
            }
            catch {
              case ex: Exception => newErrorMessage(ex)
            }

          case None =>
            newErrorMessage(new InvalidMessageException("Received a request without a registered message type: %s".format(norbertMessage.getMessageName)))
        }
      }
    }
  }
}
