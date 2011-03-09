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
package jmx

import collection.mutable.{Map, Queue}
import norbertutils._
import annotation.tailrec
import math._

class FinishedRequestTimeTracker(clock: Clock, interval: Long) {
  private val q = Queue[(Long, Int)]() // (When the request completed, request processing time)
  private var t = 0L

  private def clean {
    while(!q.isEmpty) {
      val (completion, processingTime) = q.head
      if(clock.getCurrentTime - completion > interval) {
        t -= processingTime
        q.dequeue
      } else {
        return
      }
    }
  }

  def addTime(processingTime: Int) {
    q.enqueue( (clock.getCurrentTime, processingTime) )
    t += processingTime
  }

  def total: Long = {
    clean
    t
  }

  // TODO: We just sort the data in the queue (Hey, Swee did it). Consider tracking this stuff in a sorted map.
  def percentile(p: Double): Double = {
    clean
    calculatePercentile(q.flatMap(pair => Option(pair).map(_._2)).toArray.sorted, p)
  }

  def size: Int = {
    q.size
  }

  def rps: Int = {
    val now = clock.getCurrentTime
    implicit val timeOrdering: Ordering[(Long, Int)] = new Ordering[(Long, Int)] {
      def compare(x: (Long, Int), y: (Long, Int)) = (x._1 - y._1).asInstanceOf[Int]
    }

    val bs = binarySearch(q, (now - 1000L, 0))
    val idx = if(bs < 0) -bs - 1 else bs
    q.size - idx
  }
}

class PendingRequestTimeTracker[KeyT](clock: Clock) {
  private val unfinishedRequests = Map.empty[KeyT, Long]

  // We can have about 7 million requests outstanding before overflow.
  // Long.MAX_LONG / System.currentTimeMillis
  // TODO: Make sure dead requests get properly expired from this value
  private var t = 0L

  def total: Long = {
    val now = clock.getCurrentTime
    val s = size
    (now * s) - t
  }

  def size: Int = unfinishedRequests.size

  def getStartTime(key: KeyT) = unfinishedRequests.get(key)

  def beginRequest(key: KeyT) = {
    val now = clock.getCurrentTime
    unfinishedRequests += key -> now
    t += now
  }

  def endRequest(key: KeyT) = {
    getStartTime(key).foreach { time => t -= time }
    unfinishedRequests -= key
  }

  def percentile(p: Double) = {
    val now = clock.getCurrentTime
    calculatePercentile(unfinishedRequests.values.map(startTime => now - startTime).toArray.sorted, p)
  }

}

class RequestTimeTracker[KeyT](clock: Clock, interval: Long) {
  val finishedRequestTimeTracker = new FinishedRequestTimeTracker(clock, interval)
  val pendingRequestTimeTracker = new PendingRequestTimeTracker[KeyT](clock)

  def beginRequest(key: KeyT) {
    pendingRequestTimeTracker.beginRequest(key)
  }

  def endRequest(key: KeyT) {
    pendingRequestTimeTracker.getStartTime(key).foreach { startTime =>
      finishedRequestTimeTracker.addTime((clock.getCurrentTime - startTime).asInstanceOf[Int])
    }
    pendingRequestTimeTracker.endRequest(key)
  }
}