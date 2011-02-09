package com.linkedin.norbert.network.common

import com.linkedin.norbert.cluster.Node
import java.util.concurrent.atomic.AtomicLong
import com.linkedin.norbert.util.Clock
import scala.math._

/**
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

trait CanServeRequestStrategy {
  def canServeRequest(node: Node): Boolean
}

case class CompositeCanServeRequestStrategy(strategies: CanServeRequestStrategy*) extends CanServeRequestStrategy {
  def canServeRequest(node: Node): Boolean = {
    strategies.foreach{ strategy =>
      if(!strategy.canServeRequest(node))
        return false
    }
    return true
  }
}

case object AlwaysAvailableRequestStrategy extends CanServeRequestStrategy {
  def canServeRequest(node: Node) = true
}


/**
 * A simple exponential backoff strategy
 */
class BackoffStrategy(clock: Clock, minBackoffTime: Long = 100L, maxBackoffTime: Long = 3200L) extends CanServeRequestStrategy {
  @volatile var lastFailureTime = 0L
  val backoffTime = new AtomicLong(0)

  def notifyFailure {
    lastFailureTime = clock.getCurrentTime

    // Increase the backoff
    val currentBackoffTime = backoffTime.get
    val newBackoffTime = max(minBackoffTime, min(2L * currentBackoffTime, maxBackoffTime))
    backoffTime.compareAndSet(currentBackoffTime, newBackoffTime)
  }

  def canServeRequest(node: Node): Boolean = {
    val now = clock.getCurrentTime

    // If it's been a while since the last error, reset the backoff back to 0
    val currentBackoffTime = backoffTime.get
    if(currentBackoffTime != 0L && now - lastFailureTime > 2 * maxBackoffTime)
      backoffTime.compareAndSet(currentBackoffTime, 0L)

    now - lastFailureTime > backoffTime.get
  }
}
