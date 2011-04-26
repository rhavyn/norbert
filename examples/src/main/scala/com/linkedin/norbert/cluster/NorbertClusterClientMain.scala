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
package cluster

object NorbertClusterClientMain {
  private var cluster: ClusterClient = _

  def main(args: Array[String]) {
    cluster = ClusterClient(args(0), args(1), 30000)
    loop
  }

  private def loop {
    print("> ")
    var line = Console.in.readLine.trim
    while (line != null) {
      try {
        if (line.length > 0) processCommand(line)
      } catch {
        case ex: Exception => println("Error: %s".format(ex))
      }

      print("> ")
      line = Console.in.readLine.trim
    }
  }

  private def processCommand(line: String) {
    val command :: args = line.split(" ").toList.map(_.trim).filter(_.length > 0)

    command match {
      case "nodes" =>
        val ts = System.currentTimeMillis
        val nodes = cluster.nodes
        if (nodes.size > 0) println(nodes.mkString("\n")) else println("The cluster has no nodes")

      case "join" =>
        args match {
          case nodeId :: url :: Nil =>
            cluster.addNode(nodeId.toInt, url)
            println("Joined Norbert cluster")

          case nodeId :: url :: partitions =>
            cluster.addNode(nodeId.toInt, url, Set() ++ partitions.map(_.toInt))
            println("Joined Norbert cluster")

          case _ => println("Error: Invalid syntax: join nodeId url partition1 partition2...")
        }
        println("Joined Norbert cluster")

      case "leave" =>
        if (args.length < 1) {
          println("Invalid syntax: leave nodeId")
        } else {
          cluster.removeNode(args.head.toInt)
          println("Left Norbert cluster")
        }

      case "down" =>
        if (args.length < 1) {
          println("Invalid syntax: join nodeId")
        } else {
          cluster.markNodeUnavailable(args.head.toInt)
          println("Marked node offline")
        }

      case "up" =>
        if (args.length < 1) {
          println("Invalid syntax: join nodeId")
        } else {
          cluster.markNodeAvailable(args.head.toInt)
          println("Marked node online")
        }

      case "exit" => exit

      case "quit" => exit

      case msg => "Unknown command: " + msg
    }
  }

  private def exit {
    cluster.shutdown
    System.exit(0)
  }
}
