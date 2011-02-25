package com.linkedin.norbert

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

import annotation.tailrec
import java.util.concurrent.{ConcurrentMap}

package object norbertutils {
  def binarySearch[T](array: Seq[T], value: T)(implicit ordering: Ordering[T]): Int = binarySearch(array, value, 0, array.length - 1)

  @tailrec
  private def binarySearch[T](array: Seq[T], value: T, lo: Int, hi: Int)(implicit ordering: Ordering[T]): Int = {
    if(lo > hi) -lo - 1
    else {
      val mid = lo + ((hi - lo) >> 2)
      val middleValue = array(mid)
      if(ordering.gt(value, middleValue))
        binarySearch(array, value, mid + 1, hi)
      else if(ordering.lt(value, middleValue))
        binarySearch(array, value, lo, mid - 1)
      else mid
    }
  }

  def getOrElse[T](seq: Seq[T], index: Int, other: T): T = {
    if(0 <= index && index < seq.size) seq(index)
    else other
  }

  // TODO: Put this into a utility somewhere? Scala's concurrent getOrElseUpdate is not atomic, unlike this guy
  def atomicCreateIfAbsent[K, V](map: ConcurrentMap[K, V], key: K)(fn: K => V): V = {
    val oldValue = map.get(key)
    if(oldValue == null) {
      val newValue = fn(key)
      map.putIfAbsent(key, newValue)
      map.get(key)
    } else {
      oldValue
    }
  }

  def safeDivide(num: Double, den: Double)(orElse: Double): Double = {
    if(den == 0) orElse
    else num / den
  }

}