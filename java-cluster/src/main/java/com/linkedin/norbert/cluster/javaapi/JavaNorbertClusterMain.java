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
package com.linkedin.norbert.cluster.javaapi;

import com.linkedin.norbert.cluster.ClusterException;
import com.linkedin.norbert.cluster.Node;

public class JavaNorbertClusterMain {
  public static void main(String[] args) {
    try {
      ClusterConfig config = new ClusterConfig();
      config.setClusterName(args[0]);
      config.setZooKeeperUrls(args[1]);

      ClusterBootstrap bootstrap = new ClusterBootstrap(config);
      Cluster cluster = bootstrap.getCluster();
      cluster.awaitConnectionUninterruptibly();
      for (Node node : cluster.getNodes()) {
        System.out.println(node);
      }
      cluster.shutdown();
    } catch (ClusterException ex) {
      System.out.println("Caught exception: " + ex);
    }
  }
}
