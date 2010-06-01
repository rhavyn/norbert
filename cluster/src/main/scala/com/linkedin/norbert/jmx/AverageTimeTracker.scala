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

import collection.mutable.Queue

class AverageTimeTracker(size: Int) {
  private val q = Queue[Int]()
  private var n = 0

  def +=(time: Int) = addTime(time)

  def addTime(time: Int) {
    q += time
    val old = if (q.size > size) q.dequeue else 0
    n = (n - old + time)
  }

  def average: Int = if (q.size > 0) n / q.size else n
}

class RequestsPerSecondTracker {
  private var second = 0
  private var counter = 0
  private var r = 0

  def ++ {
    val currentSecond = (System.currentTimeMillis / 1000).toInt
    if (second == currentSecond) {
      counter += 1
    } else {
      second = currentSecond
      r = counter
      counter = 1
    }
  }

  def rps: Int = r
}
