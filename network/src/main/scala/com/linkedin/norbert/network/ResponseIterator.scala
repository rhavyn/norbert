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

import com.google.protobuf.Message
import java.util.concurrent.{TimeoutException, ExecutionException, TimeUnit}

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
   * Specifies whether a response is available without blocking.
   *
   * @return true if a response is available without blocking, false otherwise
   */
  def nextAvailable: Boolean

  /**
   * Retrieves the next response, if necessary waiting until a response is available.
   *
   * @return a response
   * @throws ExecutionException thrown if there was an error
   */
  @throws(classOf[ExecutionException])
  def next: Message

  /**
   * Retrieves the next response, waiting for the specified time if there are no responses available.
   *
   * @param timeout how long to wait before giving up, in terms of <code>unit</code>
   * @param unit the <code>TimeUnit</code> that <code>timeout</code> should be interpreted in
   * 
   * @return a response
   * @throws ExecutionException thrown if there was an error
   * @throws TimeoutException thrown if a response wasn't available before the specified timeout
   * @throws InterruptedException thrown if the thread was interrupted while waiting for the next response
   */
  @throws(classOf[ExecutionException])
  @throws(classOf[TimeoutException])
  @throws(classOf[InterruptedException])
  def next(timeout: Long, unit: TimeUnit): Message
}
