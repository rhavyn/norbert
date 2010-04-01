What is Norbert
===============

Norbert is a library which provides easy cluster management and cluster aware client/server networking APIs.  Implemented in Scala, Norbert wraps ZooKeeper, Netty and uses Protocol Buffers for transport to make it easy to build a cluster aware application.  A Java API is provided and pluggable load balancing strategies are supported with round robin and consistent hash strategies provided out of the box.

Building
----------

Norbert can be built using Maven.

Using Norbert for cluster management
------------------------------------

Norbert provides a simple API to interact with a cluster and to receive notifications when the cluster topology changes.

### What is a cluster?

In Norbert a cluster is a named set of Nodes.

A Node is Norbert's representation of a service which can handle requests.  A Node contains:

1. A numerical id assigned by the client creating the Node. Norbert does not auto assign Node ids.
2. The URL to use to contact the node.
3. Optionally, one or more partition ids, representing the particular partitions the services handles.

If an application is designed around a partitioned data set or workload then each node can be assigned partition ids.  These partition ids can be used by Norbert's networking layer's partitioned load balancer functionality.

The set of member Nodes in a given cluster is reliably stored in ZooKeeper.  Additionally, a Node can advertise that it is available to process requests.  In general, a Node can be in one of three states:

1. A member of the cluster, but not advertised as available.  In this state the other nodes in the cluster know the node exists, but should not attempt to send it traffic
2. Not a member of the cluster, but available.  In this state, the node can handle requests, but it is unknown to the other nodes in the cluster.
3. A member of the cluster and available.  In this state the node is known in the cluster and it should be sent traffic.

Number 1 is most commonly the case that an administrator has specified the node in the cluster metadata, but the node is currently offline.  Number 2 is useful when the node is online, but for whatever reason, an administrator does not want it to receive traffic.

### Defining the cluster

The easiest way to define a cluster is to use the `NorbertClusterClientMain` command line program which can be found in the examples sub-directory.  At the prompt you can type

* nodes - lists all the nodes in the cluster
* join nodeId hostname port partitionId1 partitionId2 ... - adds a new node to the cluster with the given id, host, port and partitions ids
* leave nodeId - removes the node with the given id from the cluster

Under the covers, the `NorbertNetworkClientMain` command line program simply uses the `addNode` and `removeNode` methods on the `Cluster` trait.  These methods create ZNodes in ZooKeeper which store the Node's hostname/port and partition mapping metadata. Custom tools can be written using those methods in your own code.

### Interacting with the cluster

Norbert provides two ways to interact with the cluster.

1. The `ClusterClient` trait provides methods for retrieving the current data about the cluster.
2. The ClusterListener notification system allows you to register a `ClusterListener` with the cluster. The cluster will then send notifications to your `ClusterListener`s whenever the state of the cluster changes.

### Writing the code - the Scala version

    object NorbertClient {
      def main(args: Array[String]) {
        val cc = ClusterClient("norbert", "localhost:2181", 30000) (1)
        cc.awaitConnectionUninterruptibly (2)
        cc.nodes (3)
        cc.addListener(new MyClusterListener) (4)
        cc.markNodeAvailable(1) (5)
        cc.shutdown (6)
      }
    }

1. The `ClusterClient` companion object provides an easy way to instantiate and start a `ClusterClient` instance.
2. Before using the ClusterClient you must wait for it to finish connecting.
3. At this point, the cluster is usable and you can retrieve the list of cluster nodes.
4. Alternatively, instead of step 3, you can register `ClusterListener`s with the cluster and they will be sent notifications when the state of the cluster changes.
5. (Optional) If you are a member of the cluster, you want to advertise that you are available to receive traffic.
6. `shutdown` properly cleans up the resources Norbert uses and disconnects you from the cluster.

### Writing the code - the Java version

    public class NorbertClient {
      public static void main(String[] args) {
        ClusterClient cc = new ZooKeeperClusterClient("norbert", "localhost:2181", 30000); (1)
        cc.awaiteConnectionUninterruptibly(); (2)
        cc.getNodes(); (3)
        cc.addListener(new MyClusterListener()); (4)
        cc.markNodeAvailable(1); (5).
        cluster.shutdown(6)
      }
    }

1. There are currently two ClusterClient implementations in Norbert, this code is instantiating the one that uses ZooKeeper.
2. Before using the ClusterClient you must wait for it to finish connecting.
3. At this point, the cluster is usable and you can retrieve the list of cluster nodes.
4. Alternatively, instead of step 3, you can register `ClusterListener`s with the cluster and they will be sent notifications when the state of the cluster changes.
5. (Optional) If you are a member of the cluster, you want to advertise that you are available to receive traffic.
6. `shutdown` properly cleans up the resources Norbert uses and disconnects you from the cluster.

### Configuration parameters

Both the Scala and Java `ClusterClient`s take three parameters:

1. serviceName - the name of the service that runs on the cluster. This name will be used as the name of a ZooKeeper ZNode and so should be valid for that use
2. zooKeeperConnectString - the connection string passed to ZooKeeper
3. zooKeeperSessionTimeoutMillis - the session timeout passed to ZooKeeper in milliseconds

Using Norbert for client/server communication
---------------------------------------------

In addition to the cluster management, Norbert provides an API for building cluster aware client/server applications.

### Defining the API

Norbert's client/server library uses message passing semantics and, specifically, Protocol Buffers to encode those messages.  To use Norbert's client/server library, you will need to define the Protocol Buffers you will use as requests, and the associated Protocol Buffers that will be received as responses to those requests.

### Load Balancers

Norbert uses a software load balancer mechanism to route a request from a client to a server. Both partitioned and unpartitioned clusters are supported.
 
If you are building a service which will use an unpartitioned cluster, you must provide your `NetworkClient` instance with a `LoadBalancerFactory`. The `LoadBalancerFactory` is used to create `LoadBalancer` instance that will be used to route requests.  A round robin load balancer factory is provided. 

If you are building a partitioned cluster then you will want to use the `PartitionedNetworkClient` and a `PartitionedLoadBalancerFactory`. These are generic classes that have a PartitionedId type parameter. PartitionedId is the type of the id that you use to partition your cluster (e.g. a member id). A consistent hash load balancer factory is provided.

### Writing a network server - the Scala version

    object NorbertNetworkServer {
      def main(args: Array[String]) {
        val config = new NetworkServerConfig (1)
        config.serviceName = "norbert"
        config.zooKeeperConnectString = "localhost:2181"
        config.zooKeeperSessionTimeoutMillis = 30000
        config.requestThreadCorePoolSize = 5
        config.requestThreadMaxPoolSize = 10
        config.requestThreadKeepAliveTimeSecs = 300
        
        val server = NetworkServer(config) (2)
        server.registerHandler(MyRequestMessage.getDefaultInstance, MyResponseMessage.getDefaultInstance, messageHandler _) (3)
        server.bind(nodeId) (4)
      }
      
      private def messageHandler(message: Message): Message = {
        // application logic which returns a MyResponseMessage
      }
    }

1. A `NetworkServerConfig` contains the configuration data for a `NetworkServer`.
2. The `NetworkServer` companion object provides an easy to instantiate a new `NetworkServer` instance.
3. The request message, response message and the handler to call when a request message is received must be registered before using the `NetworkServer`. A single `NetworkServer` instance can handle multiple request/response/handlers.
4. Finally you bind the `NetworkServer` to the network by providing the id of the `Node` this server handles requests for. Bind will create a socket, bind it to the port specified in the `Node`'s url and mark the `Node` available in the cluster.  After this call the `NetworkServer` can begin to receive requests.

### Writing a network server - the Java version

    public class NorbertNetworkServer {
      public static void main(String[] args) {
        NetworkServerConfig config = new NetworkServerConfig();
        config.setServiceName("norbert");
        config.setZooKeeperConnectString("localhost:2181");
        config.setZooKeeperSessionTimeoutMillis(30000);
        config.setRequestThreadCorePoolSize(5);
        config.setRequestThreadMaxPoolSize(10);
        config.setRequestThreadKeepAliveTimeSecs(300);
        
        NetworkServer ns = new NettyNetworkServer(config);
        ns.registerHandler(MyRequestMessage.getDefaultInstance(), MyResponseMessage.getDefaultInstance(), new MessageHandler());
        ns.bind(nodeId);
      }
    }

1. A `NetworkServerConfig` contains the configuration data for a `NetworkServer`.
2. `NettyNetworkServer` is currently the only implementation of `NetworkServer`.
3. The request message, response message and the handler to call when a request message is received must be registered before using the `NetworkServer`. A single `NetworkServer` instance can handle multiple request/response/handlers.
4. Finally you bind the `NetworkServer` to the network by providing the id of the `Node` this server handles requests for. Bind will create a socket, bind it to the port specified in the `Node`'s url and mark the `Node` available in the cluster.  After this call the `NetworkServer` can begin to receive requests.

### Server configuration Parameters

* serverName - the name of the service that runs on the cluster
* zooKeeperConnectString - the connection string passed to ZooKeeper
* zooKeeperSessionTimeoutMillis - the session timeout passed to ZooKeeper in milliseconds
* clusterClient - as an alternative the the prior configuration parameters, you can create a `ClusterClient` instance yourself and have the `NetworkServer` use that instance by setting this field
* requestThreadCorePoolSize - the core size of the thread pool used to execute requests
* requestThreadMaxPoolSize - the maximum size of the thread pool used to execute requests
* requestThreadKeepAliveTimeSecs - the length of time in seconds to keep an idle request thread alive

### Writing the client code - the Scala version

    object NorbertNetworkClient {
      def main(args: Array[String]) {
        val config = new NetworkClientConfig (1)
        config.serviceName = "norbert"
        config.zooKeeperConnectString = "localhost:2181"
        config.zooKeeperSessionTimeoutMillis = 30000
        config.connectTimeoutMillis = 1000
        config.writeTimeoutMillis = 150
        config.maxConnectionsPerNode = 5
        config.staleRequestTimeoutMins = 10
        config.staleRequestCleanupFrequenceMins = 10
    
        val nc = NetworkClient(config, new RoundRobinLoadBalancerFactory) (2)
        OR
        val nc = PartitionedNetworkClient(config, new IntegerConsistentHashPartitionedLoadBalancerFactory)
        
        nc.registerRequest(MyRequestMessage.getDefaultInstance(), MyResponseMessage.getDefaultInstance()) (3)
    
        val f = nc.sendMessage(myRequestMessageInstance) (4)
        OR
        val f = nc.sendMessage(1210, myRequestMessageInstance)
    
        try {
          val response = f.get(500, TimeUnit.MILLISECONDS).asInstanceOf[MyResponseMessage] (5)
          // do something with the response
        } catch {
          case ex: TimeoutException => println("Timed out")
          case ex: ExecutionException => println("Error: %s".format(ex.getCause))
        }    
      }
    }

1. A `NetworkClientConfig` contains the configuration data for a `NetworkClient`.
2. The `NetworkClient` companion object provides an easy to instantiate a new `NetworkClient` instance. Alternatively the `PartitionedNetworkClient` companion object provides the same functionality for `PartitionedNetworkClient`s.
3. The request messages and response messages must be registered before using the `NetworkClient`.
4. At this point the client can be used to send messages. In the case of a `NetworkClient` the configured load balancer will be used to send the  provided message to an available `Node` in the cluster. In the case of a `PartitionedNetworkClient` the passed in id will be passed to the configured partitioned load balancer to calculate the correct node to send the message to.
5. Finally, the response can be retrieved from the returned future.

### Writing the client code - the Java version

    public class NorbertNetworkClient {
      public static void main(String[] args) {
        NetworkClientConfig config = new NetworkClientConfig(); (1)
        config.setServiceName("norbert");
        config.setZooKeeperConnectString("localhost:2181");
        config.setZooKeeperSessionTimeoutMillis(30000);
        config.setConnectTimeoutMillis(1000);
        config.setWriteTimeoutMillis(150);
        config.setConnectionsPerNode(5);
        config.setStaleRequestTimeoutMins(10);
        config.setStaleRequestCleanupFrequenceMins10);

        NetworkClient nc = new NettyNetworkClient(config, new RoundRobinLoadBalancerFactory()); (2)
        OR
        PartitionedNetworkClient<Integer> nc = new NettyPartitionedNetworkClient<Integer>(config, new IntegerConsistentHashPartitionedLoadBalancerFactory());
        
        nc.registerRequest(MyRequestMessage.getDefaultInstance(), MyResponseMessage.getDefaultInstance()); (3)

        Future<Message> f = nc.sendMessage(myRequestMessageInstance); (4)
        OR
        Future<Message> f = nc.sendMessage(1210, myRequestMessageInstance);
      
        try {
          MyResponseMessage response = (MyResponseMessage) f.get(500, TimeUnit.MILLISECONDS); (5)
          // do something with the response
        } catch (InterruptedException e) {
          e.printStackTrace();
        } catch (ExecutionException e) {
          e.printStackTrace();
        } catch (TimeoutException e) {
          e.printStackTrace();
        }
      }
    }

1. A `NetworkClientConfig` contains the configuration data for a `NetworkClient`.
2. `NettyNetworkClient` and `NettyPartitionedNetworkClient` are currently the only implementations of `NetworkClient` and `PartitionedNetworkClient` respectively.
3. The request messages and response messages must be registered before using the `NetworkClient`.
4. At this point the client can be used to send messages. In the case of a `NetworkClient` the configured load balancer will be used to send the  provided message to an available `Node` in the cluster. In the case of a `PartitionedNetworkClient` the passed in id will be passed to the configured partitioned load balancer to calculate the correct node to send the message to.
5. Finally, the response can be retrieved from the returned future.

### Configuration Parameters

* serverName - the name of the service that runs on the cluster
* zooKeeperConnectString - the connection string passed to ZooKeeper
* zooKeeperSessionTimeoutMillis - the session timeout passed to ZooKeeper in milliseconds
* clusterClient - as an alternative the the prior configuration parameters, you can create a `ClusterClient` instance yourself and have the `NetworkServer` use that instance by setting this field
* connectTimeoutMillis - the maximum number of milliseconds to allow a connection attempt to take 
* writeTimeoutMillis - the number of milliseconds a request can be queued for write before it is considered stale
* maxConnectionsPerNode - the maximum number of open connections to a node. The total number of connections that can be opened by a network client is maxConnectionsPerNode * number of nodes
* staleRequestTimeoutMins - the number of minutes to keep a request that is waiting for a response
* staleRequestCleanupFrequenceMins - the frequency to clean up stale requests
