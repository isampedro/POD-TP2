package ar.edu.itba.pod.api.client;

import ar.edu.itba.pod.api.Neighborhood;
import ar.edu.itba.pod.api.Tree;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;

public class HazelcastManager {

    private static final String TREE_NAMESPACE = "tree_collection";
    private static final String NEIGHBORHOOD_NAMESPACE = "neighborhood_collection";

    public static HazelcastInstance instanceClient(String addresses, QueryData queryData) {
        final ClientConfig ccfg = new ClientConfig();
        ccfg.getGroupConfig().setName("g14-cluster").setPassword("fruta");
        final String[] splitedAddresses = addresses.split(";");
        final ClientNetworkConfig net = new ClientNetworkConfig();
        net.addAddress(splitedAddresses);
        ccfg.setNetworkConfig(net);

        final HazelcastInstance client = HazelcastClient.newHazelcastClient(ccfg);

        IList<Tree> trees = client.getList(TREE_NAMESPACE);
        trees.addAll(queryData.getTrees());
        IList<Neighborhood> neighborhoods = client.getList(NEIGHBORHOOD_NAMESPACE);
        neighborhoods.addAll(queryData.getNeighborhoods());
        return client;
    }

    public static String getTreeNamespace() {
        return TREE_NAMESPACE;
    }

    public static String getNeighborhoodNamespace() {
        return NEIGHBORHOOD_NAMESPACE;
    }
}
