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

import com.linkedin.norbert.NorbertException

/**
 * Base class from which Norbert's networking related exceptions inherit.
 */
class NetworkingException(message: String, cause: Throwable) extends NorbertException(message, cause) {
  def this() = this(null, null)
  def this(message: String) = this(message, null)
  def this(cause: Throwable) = this(cause.getMessage, cause)
}

/**
 * Exception that indicates that an exception occurred remotely while processing a request.
 */
class RemoteException(className: String, errorMsg: String) extends NetworkingException("The remote end threw an exception [%s]: %s".format(className, errorMsg))

/**
 * Exception that indicates that a message was received which was not registered with the <code>MessageRegistry</code>.
 */
class InvalidMessageException(errorMsg: String) extends NetworkingException(errorMsg)

/**
 * Exception that indicates that a malformed response was received. 
 */
class InvalidResponseException(errorMsg: String) extends NetworkingException(errorMsg)

/**
 * Exception that indicates that a method has been called before the networking system has been started.
 */
class NetworkNotStartedException extends NetworkingException
