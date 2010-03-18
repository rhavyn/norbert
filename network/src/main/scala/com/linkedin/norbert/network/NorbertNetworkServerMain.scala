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

import com.google.protobuf.Message
import org.jboss.netty.logging.{InternalLoggerFactory, Log4JLoggerFactory}
import com.linkedin.norbert.protos.NorbertProtos
import com.linkedin.norbert.cluster.ClusterDefaults
import loadbalancer.NullRouterFactory

object NorbertNetworkServerMain {
//  InternalLoggerFactory.setDefaultFactory(new Log4JLoggerFactory)
//
//  def main(args: Array[String]) {
//    val main = new Main(args(0), args(1), args(2).toInt)
//    main.loop
//  }
//
//  private class Main(clusterName: String, zooKeeperUrls: String, nodeId: Int) {
//    println("Connecting to cluster...")
//
//    object ComponentRegistry extends {
//      val zooKeeperSessionTimeout = ClusterDefaults.ZOOKEEPER_SESSION_TIMEOUT
//      val clusterDisconnectTimeout = ClusterDefaults.CLUSTER_DISCONNECT_TIMEOUT
//      val maxConnectionsPerNode = NetworkDefaults.MAX_CONNECTIONS_PER_NODE
//      val writeTimeout = NetworkDefaults.WRITE_TIMEOUT
//      val clusterName = Main.this.clusterName
//      val zooKeeperConnectString = Main.this.zooKeeperUrls
//      val requestThreadTimeout = NetworkDefaults.REQUEST_THREAD_TIMEOUT
//      val maxRequestThreadPoolSize = NetworkDefaults.MAX_REQUEST_THREAD_POOL_SIZE
//      val coreRequestThreadPoolSize = NetworkDefaults.CORE_REQUEST_THREAD_POOL_SIZE
//    } with NullRouterFactory with DefaultNetworkServerComponent {
//      val messageRegistry = new DefaultMessageRegistry(Array((NorbertProtos.Ping.getDefaultInstance, pingHandler _)))
//      val networkServer = new NettyNetworkServer(nodeId)
//    }
//
//    import ComponentRegistry._
//
//    def loop {
//      try {
//        networkServer.bind
//        println("Connected to cluster and listening for requests")
//      } catch {
//        case ex: NetworkingException =>
//          println("Unable to bind to port, exiting: " + ex)
//          cluster.shutdown
//          System.exit(1)
//      }
//
//      Runtime.getRuntime.addShutdownHook(new Thread {
//        override def run = shutdown
//      })
//    }
//
//    private def shutdown {
//      println("Shutting down")
//      networkServer.shutdown
//    }
//
//    private def pingHandler(message: Message): Option[Message] = Some(message)
//  }
}
