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
import norbertutils.{SystemClock, MockClock, Clock, ClockComponent}

class AverageTimeTrackerSpec extends Specification {
  "RequestTimeTracker" should {
    "correctly average the times provided" in {
      val a = new FinishedRequestTimeTracker(MockClock, 100)
      (1 to 100).foreach{ t =>
        a.addTime(t)
        MockClock.currentTime = t
      }
      a.total must be_==(5050)

      a.addTime(101)
      MockClock.currentTime = 101

      a.total must be_==(5150) // first one gets knocked out
    }

    "Correctly calculate unfinished times" in {
       val tracker = new PendingRequestTimeTracker[Int](MockClock)

       (0 until 10).foreach { i =>
         MockClock.currentTime = 1000L * i
         tracker.beginRequest(i)
         (tracker.total / (i + 1)) must be_==(1000L * i / 2)
       }
    }

  }
}