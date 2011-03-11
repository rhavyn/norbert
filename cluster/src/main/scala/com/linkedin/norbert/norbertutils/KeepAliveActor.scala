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

package com.linkedin.norbert.norbertutils

import com.linkedin.norbert.logging.Logging
import actors.{DaemonActor, Actor}

object KeepAliveActor extends DaemonActor with Logging {
  def act() = loop {
    react {
      case scala.actors.Exit(actor: Actor, cause: Throwable) =>
        log.error(cause, "Actor " + actor + " seems to have died. Restarting.")
        actor.restart
        actor ! LinkActor(this)

      case _ =>
    }
  }

  this.start
  this.trapExit = true
}

case class LinkActor(actor: Actor)
