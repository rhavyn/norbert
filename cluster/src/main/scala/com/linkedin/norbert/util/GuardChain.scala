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
package util

object GuardChain {
  def apply[A](predicate: => Boolean, otherwise: => A): GuardChain[A] = new GuardChain[A] {
    def and[B >: A](gc: GuardChain[B]) = if (predicate) gc else new NoOpGuardChain(otherwise)
    def then[B >: A](op: => B) = if (predicate) op else otherwise
  }

  private class NoOpGuardChain[A](otherwise: => A) extends GuardChain[A] {
    def and[B >: A](gc: GuardChain[B]) = new NoOpGuardChain(otherwise)
    def then[B >: A](op: => B) = otherwise
  }
}

trait GuardChain[A] {
  def apply[B >: A](op: => B): B = then(op)
  def and[B >: A](gc: GuardChain[B]): GuardChain[B]
  def then[B >: A](op: => B): B
}
