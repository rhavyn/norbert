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

import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Log4JLoggerFactory;

public class NorbertJavaNetworkServerMain {
  public static void main(String[] args) {
    InternalLoggerFactory.setDefaultFactory(new Log4JLoggerFactory());

    NetworkServerConfig config = new NetworkServerConfig();
    config.setServiceName(args[0]);
    config.setZooKeeperConnectString(args[1]);
    config.setZooKeeperSessionTimeoutMillis(30000);
    final NetworkServer ns = new NettyNetworkServer(config);
    ns.registerHandler(new PingHandler(), new PingSerializer());
    ns.bind(Integer.parseInt(args[2]));

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        ns.shutdown();
      }
    });
  }

  private static class PingHandler implements RequestHandler<Ping, Ping> {
    public Ping handleRequest(Ping ping) throws Exception {
      System.out.printf("Requested ping from client %d milliseconds ago (assuming synchronized clocks)", (ping.timestamp - System.currentTimeMillis()));
      return new Ping(System.currentTimeMillis());
    }
  }
}
