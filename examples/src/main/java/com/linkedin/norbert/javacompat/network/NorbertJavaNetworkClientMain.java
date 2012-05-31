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
package com.linkedin.norbert.javacompat.network;

import com.linkedin.norbert.javacompat.cluster.ClusterClient;
import com.linkedin.norbert.javacompat.cluster.Node;
import com.linkedin.norbert.javacompat.cluster.ZooKeeperClusterClient;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Log4JLoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class NorbertJavaNetworkClientMain {
  public static void main(String[] args) {
    InternalLoggerFactory.setDefaultFactory(new Log4JLoggerFactory());

    ClusterClient cc = new ZooKeeperClusterClient(null, args[0], args[1], 30000);
    NetworkClientConfig config = new NetworkClientConfig();
    config.setClusterClient(cc);
    NetworkClient nc = new NettyNetworkClient(config, new RoundRobinLoadBalancerFactory());
//    PartitionedNetworkClient<Integer> nc = new NettyPartitionedNetworkClient<Integer>(config, new IntegerConsistentHashPartitionedLoadBalancerFactory());
//    nc.registerRequest(NorbertExampleProtos.Ping.getDefaultInstance(), NorbertExampleProtos.PingResponse.getDefaultInstance());

    Node node = cc.getNodeWithId(1);

    Future<Ping> f = nc.sendRequestToNode(new Ping(System.currentTimeMillis()), node, new PingSerializer());
    try {
      Ping pong = f.get(750, TimeUnit.MILLISECONDS);
      System.out.println(String.format("Ping took %dms", System.currentTimeMillis() - pong.timestamp));
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    } catch (TimeoutException e) {
      e.printStackTrace();
    }

    cc.shutdown();
  }
}
