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
package partitioned
package loadbalancer

/**
 * Object which provides hash function implementations.
 */
object HashFunctions {
  /**
   * An implementation of the FNV hash function.
   *
   * @param bytes the bytes to hash
   *
   * @return the hashed value of the bytes
   *
   * @see http://en.wikipedia.org/wiki/Fowler-Noll-Vo_hash_function
   */
  def fnv[T <% Array[Byte]](bytes: T): Int = {
    val FNV_BASIS = 0x811c9dc5
    val FNV_PRIME = (1 << 24) + 0x193

    var hash: Long = FNV_BASIS
    var i: Int = 0
    var maxIdx: Int = bytes.length

    while (i < maxIdx) {
      hash = (hash ^ (0xFF & bytes(i))) * FNV_PRIME
      i += 1
    }
    hash.toInt
  }
}
