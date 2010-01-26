What is Norbert
===============

Norbert is a library which provides easy cluster management and cluster aware client/server networking APIs.  Implemented in Scala, Norbert wraps ZooKeeper, Netty and uses Protocol Buffers for transport to make it easy to build a cluster aware application.  A Java API is provided and pluggable routing strategies are supported with a consistent hash strategy provided out of the box.

Using Norbert for cluster management
------------------------------------

Norbert provides a simple API to interact with a cluster and to receive notifications when the cluster topology changes.

### What is a cluster?

In Norbert, a cluster is a named set of Nodes.

A Node is Norbert's representation of a service which can handle requests.  A Node contains:

1. A numerical id assigned by the client creating the Node. Norbert does not auto assign Node ids.
2. The hostname of the machine running the service
3. The port on the host to connect to
4. The ids of the partitions the service can handle requests for

The way that data/workload is partitioned is defined by the application, but each partition must be given a numerical id.  These ids are stored in the Node and are used by Norbert's id to Node mapping feature.  Norbert can work with unpartitioned services by defining each Node as having a single partition, each with the same partition id.

The set of member Nodes in a given cluster is centrally stored in ZooKeeper.  Additionally, a Node can advertise that it is available to process requests.  In general, a Node can be in one of three states:

1. A member of the cluster, but not advertised as available.  In this state the other nodes in the cluster know the node exists, but should not attempt to send it traffic
2. Not a member of the cluster, but available.  In this state, the node can handle requests, but it is unknown to the other nodes in the cluster.
3. A member of the cluster and available.  In this state the node is known in the cluster and it should be sent traffic.

Number 1 is most commonly the case that an administrator has specified the node in the cluster metadata, but the node is currently offline.  Number 2 is useful when the node is online, but for whatever reason, an administrator does not want it to receive traffic.

### Defining the cluster

The easiest way to define a cluster is to use the `NorbertNetworkClientMain` command line program.  At the prompt you can type

* nodes - lists all the nodes in the cluster
* join nodeId hostname port partitionId1 partitionId2 ... - adds a new node to the cluster with the given id, host, port and partitions ids
* leave nodeId - removes the node with the given id from the cluster

Under the covers, the `NorbertNetworkClientMain` command line program simply uses the `addNode` and `removeNode` methods on the `Cluster` trait.  These methods create ZNodes in ZooKeeper which store the Node's hostname/port and partition mapping metadata. Custom tools can be written using those methods in your own code.

### Interacting with the cluster

Norbert provides two ways to interact with the cluster.

1. The `Cluster` trait provides methods for retrieving the current data about the cluster.
2. The ClusterListener notification system allows you to register a `ClusterListener` with the cluster. The cluster will then send notifications to your `ClusterListener`s whenever the state of the cluster changes.

### Mapping an id to a Node

If your cluster nodes are partitioned you need a way to map the id of an entity in your application to the Node that can handle requests for that id.  Norbert provides a `RouterFactory` trait which your application can extend to provide that mapping.  Whenever the cluster topology changes, the cluster will call `newRouter` on your `RouterFactory` with the list of currently available nodes.  The returned `Router` can then be retrieved from the `Cluster` instance or, if you are using the ClusterListener notification system, the `Router` is sent in the notifications.  If using the Scala API, the id mapped to a node can be of any type, the `RouterFactoryComponent` uses an abstract type to define `Router`.  The Java API is limited to using an int as an id.  Norbert comes with a `ConsistentHashRouterFactory` out of the box.

### Writing the code - the Scala version

Whether you are a consumer or a member of the cluster, you connect to the cluster in the same way.

    object NorbertClient {
      def main(args: Array[String]) {
        object ComponentRegistry extends {
          val zooKeeperSessionTimeout = ClusterDefaults.ZOOKEEPER_SESSION_TIMEOUT
          val clusterDisconnectTimeout = ClusterDefaults.CLUSTER_DISCONNECT_TIMEOUT
          val clusterName = args[0]
          val zooKeeperUrls = args[1]
        } with DefaultClusterComponent { (1)
          type Id = Int (2)
          val routerFactory = new MyRouterFactoryImplementation
        }

        import ComponentRegistry.cluster

        cluster.start (3)
        cluster.awaitConnectionUninterruptibly (4)
        cluster.nodes (5)
        cluster.router
        cluster.addListener(new MyClusterListener) (6)
        cluster.markNodeAvailable(argv[2].toInt) (7)

        cluster.shutdown (8)
      }
    }

1. Norbert uses the Cake Pattern for dependency injection and this block passes in the initialization parameters and wires up Norbert.
2. (Optional) If you are using Norbert's routing facility you define your `RouterFactory` here.  If you are using the Cake Pattern for your own application, you could wire up your components here as well.
3. Before using it, you must call `start` on the cluster instance.
4. Finally, you wait for the cluster connection to be established.
5. At this point, the cluster is usable and you can retrieve the list of cluster nodes, the current router instance, etc.
6. Alternatively, instead of steps 4 and 5 above, you can register `ClusterListener`s with the cluster and they will be sent notifications when the state of the cluster changes.
7. (Optional) If you are a member of the cluster, you want to advertise that you are available to receive traffic.
8. `shutdown` properly cleans up the resources Norbert uses and disconnects you from ZooKeeper.

### Writing the code - the Java version

    public class NorbertClient {
      public static void main(String[] args) {
        ClusterConfig config = new ClusterConfig();
        config.setClusterName(args[0]);
        config.setZooKeeperUrls(args[1]); (1)
        config.setRouterFactory(new MyRouterFactoryImplementation()); (2)

        ClusterBootstrap bootstrap = new ClusterBootstrap(config); (3)
        Cluster cluster = bootstrap.getCluster(); (4)
        cluster.awaitConnectionUninterruptibly(); (5)
        cluster.getNodes(); (6)
        cluster.getRouter();
        cluster.addListener(new MyClusterListener()); (7)
        cluster.markNodeAvailable(1); (8)

        cluster.shutdown(); (9)
      }
    }

1. The `ClusterConfig` is a JavaBean which stores the configuration data for a cluster instance.
2. (Optional) If you are using Norbert's routing facility you define your `RouterFactory` here.
3. The `ClusterBootstrap` handles initializing and configuring the cluster.
4. A new cluster instance is retrieved from the `ClusterBootstrap`.
5. Finally, you wait for the cluster connection to be established.
6. At this point, the cluster is usable and you can retrieve the list of cluster nodes, the current router instance, etc.
7. Alternatively, instead of steps 4 and 5 above, you can register `ClusterListener`s with the cluster and they will be sent notifications when the state of the cluster changes.
8. (Optional) If you are a member of the cluster, you want to advertise that you are available to receive traffic.
9. Shutdown properly cleans up the resources Norbert uses and disconnects you from ZooKeeper.

### Configuration parameters

* clusterName - the name of the cluster. This name will be used as the name of a ZooKeeper ZNode and so should be valid for that use
* zooKeeperUrls - the connection string passed to ZooKeeper
* zooKeeperSessionTimeout - the session timeout passed to ZooKeeper
* clusterDisconnectTimeout - if you are disconnected from ZooKeeper, it is possible for you to reconnect before the ZooKeeper session timeout has elapsed.  For this reason, it is not always desirable for Norbert to consider you disconnected from cluster immediately after you are disconnected from ZooKeeper.  To handle this Norbert will wait clusterDisconnectTimeout milliseconds before sending a disconnected event to `ClusterListerener`s allowing your application to continue as though it is still connected to ZooKeeper.  However, while disconnected from ZooKeeper you will not receive notification of Nodes joining or leaving the cluster, so some care needs to be taken when setting this parameter. Once reconnected to ZooKeeper, any changes that occurred will be properly propagated to the `ClusterListener`s.
