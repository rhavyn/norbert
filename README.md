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

The easiest way to define a cluster is to use the `NorbertNetworkClientMain` command line program which can be found in the examples sub-directory.  At the prompt you can type

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
2. zooKeeperUrls - the connection string passed to ZooKeeper
3. zooKeeperSessionTimeout - the session timeout passed to ZooKeeper
