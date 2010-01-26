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
package com.linkedin.norbert.network.netty

import java.util.concurrent.Executors
import org.jboss.netty.bootstrap.{ClientBootstrap, ServerBootstrap}
import org.jboss.netty.channel.socket.nio.{NioClientSocketChannelFactory, NioServerSocketChannelFactory}

/**
 * Component that builds Netty client and server bootstraps.
 */
trait BootstrapFactoryComponent {
  val bootstrapFactory: BootstrapFactory

  class BootstrapFactory {
    private val threadPool = Executors.newCachedThreadPool

    def newServerBootstrap: ServerBootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(threadPool, threadPool))

    def newClientBootstrap: ClientBootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(threadPool, threadPool))
  }
}
