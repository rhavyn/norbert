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

import actors.Actor
import Actor._
import com.linkedin.norbert.network.MessageHandlerComponent
import com.linkedin.norbert.util.Logging
import org.jboss.netty.channel.Channel
import com.linkedin.norbert.protos.NorbertProtos

trait ChannelHandlerActorComponent {
  this: MessageHandlerComponent =>

  class ChannelHandlerActor(channel: Channel) extends Actor with Logging {
    import NettyMessages._

    def act() = {
      loop {
        react {
          case MessageReceived(ctx, e) =>
            val message = e.getMessage
            log.ifDebug("Received message: %s", message)
            messageHandler.handleMessage(message.asInstanceOf[NorbertProtos.NorbertMessage]) match {
              case Some(m) => channel.write(m)
              case None =>
            }

          case ChannelClosed(ctx, e) =>
            log.ifDebug("Channel has been closed, exiting")
            exit

          case msg => log.warn("Received unexpected message: %s", msg)
        }
      }
    }
  }
}
