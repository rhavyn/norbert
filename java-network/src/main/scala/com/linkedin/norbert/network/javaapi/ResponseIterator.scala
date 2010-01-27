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
package com.linkedin.norbert.network.javaapi

import com.google.protobuf.Message
import java.util.concurrent.TimeUnit

/**
 * A <code>Response</code> contains the response provided by a node to an outgoing request.
 */
trait Response {

  /**
   * Returns true if the request was processed successfull, false otherwise
   *
   * @return true if the request was processed successfull, false otherwise
   */
  def isSuccess: Boolean

  /**
   * If <code>isSuccess</code> returns false, <code>getCause</code> will return an exception containing the
   * reason the request failed.
   *
   * @return an exception containing the reason the request failed
   */
  def getCause: Throwable

  /**
   * If <code>isSuccess</code> returns true, <code>getMessage</code> will return the message sent in response.
   *
   * @return the message sent in response
   */
  def getMessage: Message
}

/**
 * An iterator over the responses from a network request.
 */
trait ResponseIterator {

  /**
   * Calculates whether you have iterated over all of the responses. A return value of true indicates
   * that there are more responses, it does not indicate that those responses have been received and
   * are immediately available for processing.
   *
   * @return true if there are additional responses, false otherwise
   */
  def hasNext: Boolean

  /**
   * Retrieves the next response. This method does not block waiting if there are currently no responses available.
   *
   * @return a <code>Response</code> if one is immediately available, null otherwise
   */
  def next: Response

  /**
   * Retrieves the next response, waiting for the specified time if there are no responses available.
   *
   * @param timeout how long to wait before giving up, in terms of <code>unit</code>
   * @param unit the <code>TimeUnit</code> that <code>timeout</code> should be interpreted in
   *
   * @return a <code>Response</code> if a response becomes available before the timeout, null otherwise
   */
  def next(timeout: Long, unit: TimeUnit): Response
}
