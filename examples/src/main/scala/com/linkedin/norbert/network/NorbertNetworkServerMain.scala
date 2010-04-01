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
package com.linkedin.norbert.network

import netty.{NetworkServerConfig, NettyNetworkServer}
import org.jboss.netty.logging.{InternalLoggerFactory, Log4JLoggerFactory}
import com.google.protobuf.Message
import com.linkedin.norbert.cluster.ClusterClient
import com.linkedin.norbert.protos.NorbertExampleProtos
import server.{NetworkServer, NetworkServer}

object NorbertNetworkServerMain {
  InternalLoggerFactory.setDefaultFactory(new Log4JLoggerFactory)

  def main(args: Array[String]) {
    val cc = ClusterClient(args(0), args(1), 30000)
    cc.start
    cc.awaitConnectionUninterruptibly
    cc.removeNode(1)
    cc.addNode(1, "localhost:31313", new Array[Int](0))

    val config = new NetworkServerConfig
    config.clusterClient = cc

    val ns = NetworkServer(config)

    ns.registerHandler(NorbertExampleProtos.Ping.getDefaultInstance, NorbertExampleProtos.PingResponse.getDefaultInstance, pingHandler _)

    ns.bind(args(2).toInt)

    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run = {
        cc.shutdown
      }
    })
  }

  private def pingHandler(message: Message): Message = {
    val ping = message.asInstanceOf[NorbertExampleProtos.Ping]
    NorbertExampleProtos.PingResponse.newBuilder.setTimestamp(ping.getTimestamp).build
  }
}
