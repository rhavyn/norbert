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

/**
 * A container for defaults related to the networking code.
 */
object NetworkDefaults {

  /**
   * The default maximum number of connections to be opened per node.
   */
  val MAX_CONNECTIONS_PER_NODE = 20

  /**
   * The default write timeout in milliseconds.
   */
  val WRITE_TIMEOUT = 150

  /**
   * The default frequency to clean up stale requests in minutes.
   */
  val REQUEST_CLEANUP_FREQUENCY = 10

  /**
   * The default lenght of time to wait before considering a request to be stale in minutes.
   */
  val REQUEST_TIMEOUT = 10

  /**
   * The default number of core request threads.
   */
  val CORE_REQUEST_THREAD_POOL_SIZE = Runtime.getRuntime.availableProcessors * 2

  /**
   * The default max number of core request threads.
   */
  val MAX_REQUEST_THREAD_POOL_SIZE = CORE_REQUEST_THREAD_POOL_SIZE * 5

  /**
   * The default request thread timeout in seconds.
   */
  val REQUEST_THREAD_TIMEOUT = 300
}
