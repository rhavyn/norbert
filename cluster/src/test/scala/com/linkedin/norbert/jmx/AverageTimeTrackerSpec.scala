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

import org.specs.Specification
import util.{Clock, ClockComponent}

class AverageTimeTrackerSpec extends Specification {
  "RequestTimeTracker" should {
    "correctly average the times provided" in {
      val a = new RequestTimeTracker(100)
      (1 to 100).foreach(a.addTime(_))
      a.average must be_==(50)
      a.addTime(101)
      a.average must be_==(51)
    }

    "Correctly calculate unfinished times" in {
       val myClock = new Clock {
         var currentTime = 0L
         override def getCurrentTime = currentTime
       }

       val myCC = new ClockComponent {
         override val clock = myClock
       }

       val tracker = new UnfinishedRequestTimeTracker[Int] {
         override val clock = myClock
       }

       (0 until 10).foreach { i =>
         myClock.currentTime = 1000L * i
         tracker.beginRequest(i)
         tracker.pendingAverage must be_==(1000L * i / 2)
       }
    }

  }
}