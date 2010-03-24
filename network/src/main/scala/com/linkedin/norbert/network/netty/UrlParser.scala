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
package com.linkedin.norbert.network.netty

import com.linkedin.norbert.cluster.InvalidNodeException

trait UrlParser {
  def parseUrl(url: String): (String, Int) = try {
    val Array(a, p) = url.split(":")
    (a, p.toInt)
  } catch {
    case ex: MatchError => throw new InvalidNodeException("Invalid Node url format, must be in the form address:port")
    case ex: NumberFormatException => throw new InvalidNodeException("Invalid Node url format, must be in the form address:port", ex)
  }
}
