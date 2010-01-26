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

/**
 * A component which handles processing incoming response messages.
 */
trait ResponseHandlerComponent {
  this: MessageRegistryComponent =>

  val responseHandler: ResponseHandler

  trait ResponseHandler {
    def handleResponse(request: Request, norbertMessage: NorbertProtos.NorbertMessage) {
      if (norbertMessage.getStatus == NorbertProtos.NorbertMessage.Status.OK) {
        if (!norbertMessage.hasMessage()) {
          request.offerResponse(Left(new InvalidResponseException("Received a response without a message payload")))
        } else {
          messageRegistry.defaultInstanceForClassName(norbertMessage.getMessageName) match {
            case Some(defaultInstance) =>
              val proto = defaultInstance.newBuilderForType.mergeFrom(norbertMessage.getMessage).build
              request.offerResponse(Right(proto))

            case None =>
              request.offerResponse(Left(new InvalidResponseException("Received a response without a registered message type: %s".format(norbertMessage.getMessageName))))
          }
        }
      } else {
        val errorMsg = if (norbertMessage.hasErrorMessage()) norbertMessage.getErrorMessage else "<null>"
        request.offerResponse(Left(new RemoteException(norbertMessage.getMessageName, errorMsg)))
      }
    }
  }
}
