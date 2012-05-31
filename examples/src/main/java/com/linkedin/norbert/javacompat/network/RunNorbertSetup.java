package com.linkedin.norbert.javacompat.network;

import com.linkedin.norbert.cluster.InvalidClusterException;
import com.linkedin.norbert.javacompat.cluster.ClusterClient;
import com.linkedin.norbert.javacompat.cluster.ClusterListener;
import com.linkedin.norbert.javacompat.cluster.Node;
import com.linkedin.norbert.javacompat.cluster.ZooKeeperClusterClient;
import com.linkedin.norbert.network.ProtobufSerializer;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class RunNorbertSetup
{
    public static void main(String[] args)
    {
        final String serviceName = "norbert";
        final String zkConnectStr = "localhost:2181";

        configCluster(serviceName, zkConnectStr);
        startServer(serviceName, 1, zkConnectStr);
        startServer(serviceName, 2, zkConnectStr);

        NetworkClientConfig config = new NetworkClientConfig();
        config.setServiceName(serviceName);
        config.setZooKeeperConnectString(zkConnectStr);
        config.setZooKeeperSessionTimeoutMillis(30000);
        config.setConnectTimeoutMillis(1000);
        config.setWriteTimeoutMillis(150);
        config.setMaxConnectionsPerNode(5);
        config.setStaleRequestTimeoutMins(10);
        config.setStaleRequestCleanupFrequencyMins(10);

        final LoadBalancerFactory myLB = new LoadBalancerFactory()
        {
            @Override
            public LoadBalancer newLoadBalancer(final Set<Endpoint> endpoints) throws InvalidClusterException
            {
                return new LoadBalancer()
                {
                    @Override
                    public Node nextNode()
                    {
                        return endpoints.iterator().next().getNode();
                    }
                };
            }
        };
        NetworkClient nc = new NettyNetworkClient(config, new RoundRobinLoadBalancerFactory());
        //PartitionedNetworkClient<Integer> nc = new NettyPartitionedNetworkClient<Integer>(config, new IntegerConsistentHashPartitionedLoadBalancerFactory());

        //nc.registerRequest(NetqProtocol.AppendReq.getDefaultInstance(), NetqProtocol.AppendResp.getDefaultInstance());


        final Ping request = new Ping(System.currentTimeMillis());
        Future<Ping> pingFuture = nc.sendRequest(request, new PingSerializer());

        try
        {
            final Ping appendResp = pingFuture.get();
            System.out.println("got ping resp: " + appendResp);
        }
        catch( InterruptedException e )
        {
            e.printStackTrace();
        }
        catch( ExecutionException e )
        {
            e.printStackTrace();
        }

    }

    private static void startServer(String serviceName, int nodeId, String zkConnectStr)
    {
        NetworkServerConfig config = new NetworkServerConfig();
        config.setServiceName(serviceName);
        config.setZooKeeperConnectString(zkConnectStr);
        config.setZooKeeperSessionTimeoutMillis(30000);
        config.setRequestThreadCorePoolSize(5);
        config.setRequestThreadMaxPoolSize(10);
        config.setRequestThreadKeepAliveTimeSecs(300);

        NetworkServer ns = new NettyNetworkServer(config);

        ns.registerHandler(new RequestHandler<Ping, Ping>()
        {
            @Override
            public Ping handleRequest(Ping request) throws Exception
            {
                return new Ping(System.currentTimeMillis());
            }
        }, new PingSerializer());


        ns.bind(nodeId);
    }

    private static void configCluster(String serviceName, String zkConnectStr)
    {
        //ClusterClient cc = new InMemoryClusterClient("norbert");//, "localhost:2181", 30000);
        final ClusterClient cc = new ZooKeeperClusterClient(serviceName, zkConnectStr, 30000);
        cc.awaitConnectionUninterruptibly();

        cc.addListener(new ClusterListener()
        {
            @Override
            public void handleClusterConnected(Set<Node> nodes)
            {
                System.out.println("connected to cluster: " + nodes);
            }

            @Override
            public void handleClusterNodesChanged(Set<Node> nodes)
            {
                System.out.println("nodes changed: ");
                for( Node node : nodes )
                {
                    System.out.println("node: " + node);
                }
            }

            @Override
            public void handleClusterDisconnected()
            {
                final Set<Node> nodes = cc.getNodes();
                System.out.println("dis-connected from cluster: " + nodes);
            }

            @Override
            public void handleClusterShutdown()
            {
                final Set<Node> nodes = cc.getNodes();
                System.out.println("cluster shutdown: " + nodes);
            }
        });

        cc.removeNode(1);
        cc.removeNode(2);
        cc.addNode(1, "localhost:5002");
        cc.addNode(2, "localhost:5003");

//        cc.markNodeAvailable(1);
//        cc.shutdown();
    }
}
