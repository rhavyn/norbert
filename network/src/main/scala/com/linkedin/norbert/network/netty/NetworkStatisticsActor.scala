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
package netty

import jmx.{AverageTimeTracker, RequestsPerSecondTracker}
import logging.Logging
import actors.DaemonActor

class NetworkStatisticsActor(averageTimeSize: Int) extends DaemonActor with Logging {
  object Stats {
    case class NewProcessingTime(time: Int)
    case object GetAverageProcessingTime
    case class AverageProcessingTime(time: Int)
    case object GetRequestsPerSecond
    case class RequestsPerSecond(rps: Int)
  }

  private val processingTime = new AverageTimeTracker(averageTimeSize)
  private val rps = new RequestsPerSecondTracker

  def act() = {
    import Stats._

    loop {
      react {
        case NewProcessingTime(time) =>
          processingTime += time
          rps++

        case GetAverageProcessingTime => reply(AverageProcessingTime(processingTime.average))

        case GetRequestsPerSecond => reply(RequestsPerSecond(rps.rps))

        case msg => log.error("NetworkStatistics actor got invalid message: %s".format(msg))
      }
    }
  }
}
