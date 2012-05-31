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
package network

import client._
import client.loadbalancer.RoundRobinLoadBalancerFactory
import org.jboss.netty.logging.{Log4JLoggerFactory, InternalLoggerFactory}
import java.util.concurrent.{ExecutionException, TimeoutException, TimeUnit}
import cluster.{ClusterShutdownException, ClusterClient}
import protos.NorbertExampleProtos

object NorbertNetworkClientMain {
  InternalLoggerFactory.setDefaultFactory(new Log4JLoggerFactory)

  def main(args: Array[String]) {
    val cc = ClusterClient( args(0), args(1), 30000)

    val config = new NetworkClientConfig
    config.clusterClient = cc

    val nc = NetworkClient(config, new RoundRobinLoadBalancerFactory)
//    nc.registerRequest(NorbertExampleProtos.Ping.getDefaultInstance, NorbertExampleProtos.PingResponse.getDefaultInstance)

    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run = {
        cc.shutdown
      }
    })

    loop(nc, cc)
  }

  def loop(nc: NetworkClient, cc: ClusterClient) {
    print("> ")
    var line = Console.in.readLine.trim
    while (line != null) {
      try {
        if (line.length > 0) processCommand(nc, cc, line)
      } catch {
        case ex: ClusterShutdownException => throw ex
        case ex: Exception => println("Error: %s".format(ex))
      }

      print("> ")
      line = Console.in.readLine.trim
    }
  }

  def processCommand(nc: NetworkClient, cc: ClusterClient, line: String) {
    val command :: args = line.split(" ").toList.map(_.trim).filter(_.length > 0)

    command match {
      case "nodes" =>
        val nodes = cc.nodes
        if (nodes.size > 0) println(nodes.mkString("\n")) else println("The cluster has no nodes")

      case "join" =>
        args match {
          case nodeId :: url :: Nil =>
            cc.addNode(nodeId.toInt, url)
            println("Joined Norbert cluster")

          case nodeId :: url :: partitions =>
            cc.addNode(nodeId.toInt, url, Set() ++ partitions.map(_.toInt))
            println("Joined Norbert cluster")

          case _ => println("Error: Invalid syntax: join nodeId url partition1 partition2...")
        }
        println("Joined Norbert cluster")

      case "leave" =>
        if (args.length < 1) {
          println("Invalid syntax: leave nodeId")
        } else {
          cc.removeNode(args.head.toInt)
          println("Left Norbert cluster")
        }

      case "ping" =>
        if (args.length < 1) {
          println("Invalid syntax: ping nodeId")
        } else {
          val node = cc.nodeWithId(args.head.toInt)
          node match {
            case Some(n) =>
              val future = nc.sendRequestToNode(Ping(System.currentTimeMillis), n)
              try {
                val response = future.get(500, TimeUnit.MILLISECONDS).asInstanceOf[NorbertExampleProtos.PingResponse]
                println("Ping took %dms".format(System.currentTimeMillis - response.getTimestamp))
              } catch {
                case ex: TimeoutException => println("Ping timed out")
                case ex: ExecutionException => println("Error: %s".format(ex.getCause))
              }

            case None => println("No node with id: %d".format(args.head.toInt))
          }
        }

      case "exit" => System.exit(0)

      case "quit" => System.exit(0)

      case msg => "Unknown command: " + msg

    }
  }
}
