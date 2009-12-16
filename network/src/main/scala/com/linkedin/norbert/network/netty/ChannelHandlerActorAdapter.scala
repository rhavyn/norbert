/*
 * Copyright 2009 LinkedIn, Inc
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

import com.linkedin.norbert.util.Logging
import actors.Actor
import org.jboss.netty.channel._
import org.jboss.netty.channel.group.ChannelGroup

sealed trait NettyMessage
object NettyMessages {
  case class MessageReceived(ctx: ChannelHandlerContext, e: MessageEvent) extends NettyMessage
  case class ChannelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) extends NettyMessage
}

@ChannelPipelineCoverage("one")
class ChannelHandlerActorAdapter(channelGroup: ChannelGroup, actorFactory: (Channel) => Actor) extends SimpleChannelHandler
        with Logging {
  import NettyMessages._

  private var actor: Option[Actor] = None

  override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent) = {
    log.ifTrace("ChannelOpen: %s", e.getChannel)
    val channel = e.getChannel
    val a = actorFactory(channel)
    a.start
    actor = Some(a)
    channelGroup.add(channel)
    super.channelOpen(ctx, e)
  }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) = {
    log.ifTrace("MessageReceived (%s): %s", e.getChannel, e.getMessage)
    send(MessageReceived(ctx, e))
  }

  override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) = {
    log.ifTrace("ChannelClosed: %s", e.getChannel)
    send(ChannelClosed(ctx, e))
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) = log.error(e.getCause, "Uncaught exception in networking code")

  private def send(message: NettyMessage) {
    actor match {
      case Some(a) => a ! message
      case None => log.error("Actor has not been created for channel")
    }
  }
}
