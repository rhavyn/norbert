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

import java.util.concurrent.TimeUnit;

import com.google.protobuf.Message;
import com.linkedin.norbert.NorbertException;
import com.linkedin.norbert.cluster.Node;
import com.linkedin.norbert.protos.NorbertProtos;

public class JavaNetworkClientMain
{
  public static void main(String[] args)
  {
    String clusterName = args[0];
    String zookeeperUrls = args[1];
    int numPartitions = Integer.parseInt(args[2]);
    Message[] messages = { NorbertProtos.Ping.getDefaultInstance() };

    try {
      NetworkClientConfig config = new NetworkClientConfig();
      config.setClusterName(clusterName);
      config.setZooKeeperUrls(zookeeperUrls);
      config.setResponseMessages(messages);

      config.setRouterFactory(new ConsistentHashRouterFactory(numPartitions));
      ClientBootstrap bootstrap = new ClientBootstrap(config);
      NetworkClient networkClient = bootstrap.getNetworkClient();
      Cluster cluster = bootstrap.getCluster();

      for (Node node : cluster.getNodes()) {
        System.out.println(node);
      }

      int[] ids = {1};
      NorbertProtos.Ping outgoingPing = NorbertProtos.Ping.newBuilder().setTimestamp(System.currentTimeMillis()).build();
      ResponseIterator it = networkClient.sendMessage(ids, outgoingPing);

      while (it.hasNext()) {
        Response response = it.next(500, TimeUnit.MILLISECONDS);
        if (response == null) {
          System.out.println("Ping timed out");
        } else if (response.isSuccess()) {
          NorbertProtos.Ping incomingPing = (NorbertProtos.Ping) response.getMessage();
          System.out.println("Ping took " + (System.currentTimeMillis() - incomingPing.getTimestamp()) + "ms");
        } else {
          System.out.println("Error: " + response.getCause());
        }
      }

      bootstrap.shutdown();
    } catch (NorbertException ex) {
      System.out.println("Error: " + ex);
    }
  }
}
