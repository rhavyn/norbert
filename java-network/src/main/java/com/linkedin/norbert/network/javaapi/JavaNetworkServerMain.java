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
package com.linkedin.norbert.network.javaapi;

import com.google.protobuf.Message;
import com.linkedin.norbert.cluster.javaapi.ConsistentHashRouterFactory;
import com.linkedin.norbert.network.NetworkingException;
import com.linkedin.norbert.protos.NorbertProtos;

public class JavaNetworkServerMain {
  public static void main(String[] args) {
    String clusterName = args[0];
    String zookeeperUrls = args[1];
    int numPartitions = Integer.parseInt(args[2]);
    int nodeId = Integer.parseInt(args[3]);
    MessageHandler[] handlers = new MessageHandler[] { new PingMessageHandler() };

    ServerConfig config = new ServerConfig();
    config.setClusterName(clusterName);
    config.setZooKeeperUrls(zookeeperUrls);
    config.setMessageHandlers(handlers);
    config.setNodeId(nodeId);

    config.setRouterFactory(new ConsistentHashRouterFactory(numPartitions));
    ServerBootstrap bootstrap = new ServerBootstrap(config);
    NetworkServer server = bootstrap.getNetworkServer();
    
    try {
      server.bind();
    } catch (NetworkingException e) {
      System.out.println("Unable to bind to port: " + e);
      server.shutdown();
      bootstrap.shutdown();
    }
  }

  public static class PingMessageHandler implements MessageHandler {
    public Message[] getMessages() {
      return new Message[] { NorbertProtos.Ping.getDefaultInstance() };
    }

    public Message handleMessage(Message message) {
      return message;
    }
  }
}
