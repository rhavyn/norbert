package com.linkedin.norbert.network.common

import com.linkedin.norbert.cluster.Node

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

case class CompositeCanServeRequestStrategy(s1: CanServeRequestStrategy, s2: CanServeRequestStrategy) extends CanServeRequestStrategy {
  def canServeRequest(node: Node) = s1.canServeRequest(node) && s2.canServeRequest(node)
}

case object AlwaysAvailableRequestStrategy extends CanServeRequestStrategy {
  def canServeRequest(node: Node) = true
}