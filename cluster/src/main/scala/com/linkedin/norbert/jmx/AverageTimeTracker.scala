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

import norbertutils._
import collection.JavaConversions
import java.util.concurrent.atomic.AtomicInteger

// Threadsafe. Writers should always complete more or less instantly. Readers work via copy-on-write.
class FinishedRequestTimeTracker(clock: Clock, interval: Long) {
  private val q = new java.util.concurrent.ConcurrentLinkedQueue[(Long, Int)]()
  private val currentlyCleaning = new java.util.concurrent.atomic.AtomicBoolean

  private def clean {
    // Let only one thread clean at a time
    if(currentlyCleaning.compareAndSet(false, true)) {
      clean0
      currentlyCleaning.set(false)
    }
  }

  private def clean0 {
    while(!q.isEmpty) {
      val head = q.peek
      if(head == null)
        return

      val (completion, processingTime) = head
      if(clock.getCurrentTime - completion > interval) {
        q.remove(head)
      } else {
        return
      }
    }
  }

  def addTime(processingTime: Int) {
    clean
    q.offer( (clock.getCurrentTime, processingTime) )
  }

  def getArray: Array[(Long, Int)] = {
    clean
    q.toArray(Array.empty[(Long, Int)])
  }

  def getTimings: Array[Int] = {
    getArray.map(_._2).sorted
  }

  def total = {
    getTimings.sum
  }

  def reset {
    q.clear
  }
}

// Threadsafe
class PendingRequestTimeTracker[KeyT](clock: Clock) {
  private val numRequests = new AtomicInteger()

  private val map : java.util.concurrent.ConcurrentMap[KeyT, Long] =
    new java.util.concurrent.ConcurrentHashMap[KeyT, Long]

  def getStartTime(key: KeyT) = Option(map.get(key))

  def beginRequest(key: KeyT) {
    numRequests.incrementAndGet
    val now = clock.getCurrentTime
    map.put(key, now)
  }

  def endRequest(key: KeyT) {
    map.remove(key)
  }

  def getTimings = {
    val now = clock.getCurrentTime
    val timings = map.values.toArray(Array.empty[java.lang.Long])
    timings.map(t => (now - t.longValue).asInstanceOf[Int]).sorted
  }

  def reset {
    map.clear
  }

  def getTotalNumRequests = numRequests.get

  def total = getTimings.sum
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

  def reset {
    finishedRequestTimeTracker.reset
    pendingRequestTimeTracker.reset
  }
}