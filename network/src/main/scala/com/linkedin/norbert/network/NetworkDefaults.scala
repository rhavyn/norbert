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
package com.linkedin.norbert
package network

/**
 * A container for defaults related to the networking code.
 */
object NetworkDefaults {

  /**
   * The default maximum number of connections to be opened per node.
   */
  val MAX_CONNECTIONS_PER_NODE = 5

  /**
   * The default number of milliseconds to wait when opening a socket.
   */
  val CONNECT_TIMEOUT_MILLIS = 1000

  /**
   * The default write timeout in milliseconds.
   */
  val WRITE_TIMEOUT_MILLIS = 150

  /**
   * The default frequency to clean up stale requests in minutes.
   */
  val STALE_REQUEST_CLEANUP_FREQUENCY_MINS = 1

  /**
   * The default length of time to wait before considering a request to be stale in minutes.
   */
  val STALE_REQUEST_TIMEOUT_MINS = 10

  /**
   * The default number of core request threads.
   */
  val REQUEST_THREAD_CORE_POOL_SIZE = Runtime.getRuntime.availableProcessors * 2

  /**
   * The default max number of core request threads.
   */
  val REQUEST_THREAD_MAX_POOL_SIZE = REQUEST_THREAD_CORE_POOL_SIZE * 5

  /**
   * The default request thread timeout in seconds.
   */
  val REQUEST_THREAD_KEEP_ALIVE_TIME_SECS = 300

  /**
   * The default thread pool queue size for processing requests
   */
  val REQUEST_THREAD_POOL_QUEUE_SIZE = 100

  /**
   * The default number of core response processing threads.
   */
  val RESPONSE_THREAD_CORE_POOL_SIZE = Runtime.getRuntime.availableProcessors * 1

  /**
   * The default max number of core response threads.
   */
  val RESPONSE_THREAD_MAX_POOL_SIZE = REQUEST_THREAD_CORE_POOL_SIZE * 5

  /**
   * The default response processing thread timeout in seconds.
   */
  val RESPONSE_THREAD_KEEP_ALIVE_TIME_SECS = 100

  /**
   * The default thread pool queue size for processing requests
   */
  val RESPONSE_THREAD_POOL_QUEUE_SIZE = 200

  /**
   * The default window size/time (in milliseconds) for averaging processing statistics
   */
  val REQUEST_STATISTICS_WINDOW = 10000L

  /**
   *   Detects nodes that may be offline if their request processing times are greater than this multiplier over the average
   */
  val OUTLIER_MULTIPLIER = 2.0

  /**
   * Detects nodes that may be offline if their request processing times are also greater than this additional constant
   */
  val OUTLIER_CONSTANT = 10.0
}
