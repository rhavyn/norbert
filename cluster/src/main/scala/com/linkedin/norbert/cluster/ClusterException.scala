/*
 * Copyright 2009 LinkedIn, Inc
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
package com.linkedin.norbert.cluster

import com.linkedin.norbert.NorbertException

/**
 * Base class for exceptions thrown by the <code>Cluster</code>.
 */
class ClusterException(message: String, cause: Throwable) extends NorbertException(message, cause) {
  def this() = this(null, null)
  def this(message: String) = this(message, null)
  def this(cause: Throwable) = this(cause.getMessage, cause)
}

/**
 * Exception that indicates that an operation was attempted when the current node was not connected to the cluster.
 */
class ClusterDisconnectedException(message: String) extends ClusterException(message)

/**
 * Exception that indicates that an operation was attempted before <code>start</code> was called on the cluster.
 */
class ClusterNotStartedException extends ClusterException

/**
 * Exception that indicates that an operation was attempted after <code>shutdown</code> was called on the cluster.
 */
class ClusterShutdownException extends ClusterException

/**
 * Exception that indicates something was invalid on the node for which the operation was being performed.
 */
class InvalidNodeException(message: String, cause: Throwable) extends ClusterException(message) {
  def this(message: String) = this(message, null)
}

/**
 * Exception that indicates that something about the cluster was invalid when an operation was attempted.
 */
class InvalidClusterException(message: String) extends ClusterException(message)
