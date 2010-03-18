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

import java.util.concurrent.TimeUnit
import org.jboss.netty.logging.{Log4JLoggerFactory, InternalLoggerFactory}
import com.linkedin.norbert.protos.NorbertProtos
import com.linkedin.norbert.cluster.{ClusterDefaults, ClusterShutdownException}
import loadbalancer.NullRouterFactory

object NorbertNetworkClientMain {
//  InternalLoggerFactory.setDefaultFactory(new Log4JLoggerFactory)
//
//  def main(args: Array[String]) {
//    val main = new Main(args(0), args(1))
//    main.loop
//  }
//
//  private class Main(clusterName: String, zooKeeperUrls: String) {
//    println("Connecting to cluster...")
//
//    object ComponentRegistry extends {
//      val zooKeeperSessionTimeout = ClusterDefaults.ZOOKEEPER_SESSION_TIMEOUT
//      val clusterDisconnectTimeout = ClusterDefaults.CLUSTER_DISCONNECT_TIMEOUT
//      val maxConnectionsPerNode = NetworkDefaults.MAX_CONNECTIONS_PER_NODE
//      val writeTimeout = NetworkDefaults.WRITE_TIMEOUT
//      val clusterName = Main.this.clusterName
//      val zooKeeperConnectString = Main.this.zooKeeperUrls
//    } with DefaultNetworkClientFactoryComponent with NullRouterFactory {
//      val messageRegistry = new DefaultMessageRegistry(Array(NorbertProtos.Ping.getDefaultInstance))
//    }
//
//    import ComponentRegistry._
//
//    val client = networkClientFactory.newNetworkClient
//    println("Connected to cluster")
//
//    def loop {
//      print("> ")
//      var line = Console.in.readLine.trim
//      while (line != null) {
//        try {
//          if (line.length > 0) processCommand(line)
//        } catch {
//          case ex: ClusterShutdownException => throw ex
//          case ex: Exception => println("Error: %s".format(ex))
//        }
//
//        print("> ")
//        line = Console.in.readLine.trim
//      }
//    }
//
//    private def processCommand(line: String) {
//      val command :: args = line.split(" ").toList.map(_.trim).filter(_.length > 0)
//
//      command match {
//        case "nodes" =>
//          val nodes = cluster.nodes
//          if (nodes.length > 0) println(nodes.mkString("\n")) else println("The cluster has no nodes")
//
//        case "join" =>
//          if (args.length < 4) {
//            println("Error: Invalid syntax: join nodeId url partition1 partition2...")
//          } else {
//            val nodeId :: url :: partitions = args
//            cluster.addNode(nodeId.toInt, url, partitions.map(_.toInt).toArray)
//            println("Joined Norbert cluster")
//          }
//
//        case "leave" =>
//          if (args.length < 1) {
//            println("Invalid syntax: leave nodeId")
//          } else {
//            cluster.removeNode(args.head.toInt)
//            println("Left Norbert cluster")
//          }
//
//        case "ping" =>
//          if (args.length < 1) {
//            println("Invalid syntax: ping nodeId")
//          } else {
//            val node = cluster.nodeWithId(args.head.toInt)
//            node match {
//              case Some(n) =>
//                val it = client.sendMessageToNode(n, NorbertProtos.Ping.newBuilder.setTimestamp(System.currentTimeMillis).build)
//                while (it.hasNext) {
//                  it.next(500, TimeUnit.MILLISECONDS) match {
//                    case Some(Right(ping: NorbertProtos.Ping)) => println("Ping took %dms".format(System.currentTimeMillis - ping.getTimestamp))
//
//                    case Some(Left(ex)) => println("Error: %s".format(ex))
//
//                    case None => println("Ping timed out")
//                  }
//                }
//
//              case None => println("No node with id: %d".format(args.head.toInt))
//            }
//          }
//
//        case "exit" => exit
//
//        case "quit" => exit
//
//        case msg => "Unknown command: " + msg
//      }
//    }
//
//    private def exit {
//      cluster.shutdown
//      System.exit(0)
//    }
//  }
}
