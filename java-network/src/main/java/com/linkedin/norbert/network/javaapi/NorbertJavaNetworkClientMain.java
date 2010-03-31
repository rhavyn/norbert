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
import com.linkedin.norbert.cluster.javaapi.ClusterClient;
import com.linkedin.norbert.cluster.javaapi.ZooKeeperClusterClient;
import com.linkedin.norbert.protos.NorbertProtos;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Log4JLoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class NorbertJavaNetworkClientMain {
  public static void main(String[] args) {
    InternalLoggerFactory.setDefaultFactory(new Log4JLoggerFactory());

    ClusterClient cc = new ZooKeeperClusterClient("nimbus", "localhost:2181", 30000);
    NetworkClientConfig config = new NetworkClientConfig();
    config.setClusterClient(cc);
    NetworkClient nc = new NettyNetworkClient(config, new RoundRobinLoadBalancerFactory());
    nc.registerRequest(NorbertProtos.Ping.getDefaultInstance(), NorbertProtos.PingResponse.getDefaultInstance());

    Future<Message> f = nc.sendMessage(NorbertProtos.Ping.newBuilder().setTimestamp(System.currentTimeMillis()).build());
    try {
      f.get(750, TimeUnit.MILLISECONDS);
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
