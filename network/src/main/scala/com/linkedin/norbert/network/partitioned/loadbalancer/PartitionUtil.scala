package com.linkedin.norbert.network.partitioned.loadbalancer

import com.linkedin.norbert.network.common.Endpoint

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

object PartitionUtil {
  def wheelEntry[K, V](map: java.util.TreeMap[K, V], key: K): java.util.Map.Entry[K, V] = {
    val entry = map.ceilingEntry(key)
    if(entry == null)
      map.firstEntry
    else
      entry
  }

  def rotateWheel[K, V](map: java.util.TreeMap[K, V], key: K): java.util.Map.Entry[K, V] = {
    val entry = map.higherEntry(key)
    if(entry == null)
      map.firstEntry
    else
      entry
  }

  def searchWheel[T, V](wheel: java.util.TreeMap[T, V], key: T, usable: V => Boolean): Option[V] = {
    if(wheel.isEmpty)
      return None

    val entry = PartitionUtil.wheelEntry(wheel, key)
    var e = entry
    do {
      if(usable(e.getValue))
        return Some(e.getValue)

      // rotate the wheel
      e = PartitionUtil.rotateWheel(wheel, e.getKey)
    }
    while (e != entry)
    return Some(e.getValue)
  }

}