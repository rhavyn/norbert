package com.linkedin.norbert
package network
package common

import com.linkedin.norbert.cluster.Node
import java.util.concurrent.atomic.AtomicLong
import com.linkedin.norbert.norbertutils.Clock
import scala.math._
import com.linkedin.norbert.jmx.JMX.MBean
import collection.mutable.ConcurrentMap

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

trait CanServeRequestStrategyMBean {
  def canServeRequests: Map[Int, Boolean]
}

object CompositeCanServeRequestStrategy {
  def build(strategies: Option[CanServeRequestStrategy]*): CompositeCanServeRequestStrategy =
    new CompositeCanServeRequestStrategy(strategies.flatten :_*)
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
private[common] class SimpleBackoff(clock: Clock, minBackoffTime: Long = 100L, maxBackoffTime: Long = 3200L) {
  @volatile var lastError = 0L
  val currBackoff = new AtomicLong(0)

  def notifyFailure {
    if(clock.getCurrentTime - lastError >= minBackoffTime)
      incrementBackoff
  }

  private def incrementBackoff {
    lastError = clock.getCurrentTime
    val currentBackoffTime = currBackoff.get
    val newBackoffTime = max(minBackoffTime, min(2L * currentBackoffTime, maxBackoffTime))
    currBackoff.compareAndSet(currentBackoffTime, newBackoffTime)
  }

  private def tryDecrementBackoff {
   val now = clock.getCurrentTime

   // If it's been a while since the last error, reset the backoff back to 0
    val currentBackoffTime = currBackoff.get
    if(currentBackoffTime != 0L && now - lastError > 3 * maxBackoffTime)
      currBackoff.compareAndSet(currentBackoffTime, 0L)
  }

  def available: Boolean = {
    val now = clock.getCurrentTime
    tryDecrementBackoff
    now - lastError > currBackoff.get
  }
}

trait BackoffStrategy extends CanServeRequestStrategy {
  def notifyFailure(node: Node)
}

class SimpleBackoffStrategy(clock: Clock, minBackoffTime: Long = 100L, maxBackoffTime: Long = 3200L) extends BackoffStrategy {
  import norbertutils._
  import collection.JavaConversions._

  private[common] val jBackoff = new java.util.concurrent.ConcurrentHashMap[Node, SimpleBackoff]
  private[common] val backoff: ConcurrentMap[Node, SimpleBackoff] = jBackoff

  def notifyFailure(node: Node) {
    atomicCreateIfAbsent(jBackoff, node) { n =>
      new SimpleBackoff(clock, minBackoffTime, maxBackoffTime)
    }.notifyFailure
  }

  def canServeRequest(node: Node): Boolean = {
    val b = jBackoff.get(node)
    b == null || b.available
  }
}

class ServerErrorStrategyMBeanImpl(serviceName: String, ses: SimpleBackoffStrategy)
  extends MBean(classOf[ServerErrorStrategyMBeanImpl], "service=%s".format(serviceName))
  with CanServeRequestStrategyMBean {
  def canServeRequests = ses.backoff.keys.map(node => (node.id, ses.canServeRequest(node))).toMap
}