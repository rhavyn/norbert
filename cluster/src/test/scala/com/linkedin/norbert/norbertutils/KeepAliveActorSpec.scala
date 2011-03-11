package com.linkedin.norbert.norbertutils

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
import org.specs.Specification
import org.specs.mock.Mockito
import org.specs.util.WaitFor
import actors._
import actors.Actor._

class KeepAliveActorSpec extends Specification {
  case class Divide(n: Int, d: Int)

  class TestActor extends Actor {
    def act() = loop {
      react {
        case LinkActor(actor) => this.link(actor)
        case Divide(n, d) => reply(n / d)
      }
    }

    this ! LinkActor(KeepAliveActor)
  }
  "KeepAliveActor" should {
    "Keep an actor alive" in {
      val actor = new TestActor
      actor.start

      (actor !? (1000, Divide(10, 2))) must be_==(Some(5))

      actor !? (1000, Divide(10, 0)) must be_==(None)

      actor !? (1000, Divide(10, 2)) must be_==(Some(5))
    }
  }
}
