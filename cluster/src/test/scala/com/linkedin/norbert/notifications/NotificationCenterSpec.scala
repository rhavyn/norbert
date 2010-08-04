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
package notifications

import org.specs.Specification

class NotificationCenterSpec extends Specification {
  case class Notification1(id: Int) extends Notification
  case class Notification2(id: Int) extends Notification

  "NotificationCenter" should {
    "send a posted notification to all observers" in {
      val nc = new NotificationCenter
      var callCount = 0

      nc.addObserver(Observer {
        case n: Notification1 => callCount += 1
      })
      nc.addObserver(Observer {
        case n: Notification1 => callCount += 1
      })
      nc.postNotification(Notification1(1))

      callCount must eventually(be_==(2))

      nc.shutdown
    }

    "remove observers" in {
      val nc = new NotificationCenter
      var callCount1 = 0
      var callCount2 = 0

      nc.addObserver(Observer {
        case n: Notification1 => callCount1 += 1
      })
      val key = nc.addObserver(Observer {
        case n: Notification1 => callCount2 += 1
      })
      nc.postNotification(Notification1(1))

      callCount1 must eventually(be_==(1))
      callCount2 must eventually(be_==(1))

      nc.removeObserver(key)
      nc.postNotification(Notification1(1))

      callCount1 must eventually(be_==(2))
      callCount2 must be_==(1)

      nc.shutdown
    }

    "only pass a notification to an observer that is listening for it" in {
      val nc = new NotificationCenter
      var callCount1 = 0
      var callCount2 = 0

      nc.addObserver(Observer {
        case n: Notification1 => callCount1 += 1
      })
      val key = nc.addObserver(Observer {
        case n: Notification2 => callCount2 += 1
      })
      nc.postNotification(Notification1(1))

      callCount1 must eventually(be_==(1))
      callCount2 must be_==(0)

      nc.postNotification(Notification2(1))

      callCount2 must eventually(be_==(1))
      callCount1 must be_==(1)

      nc.shutdown
    }
  }
}
