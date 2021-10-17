package ar.edu.itba.pod.api.client;

import ar.edu.itba.pod.api.Tree;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class Query1 extends BasicQuery{

    public static void main(String[] args) {

        parseArguments();

        try {
            if (commonArgsNull())
               throw new IllegalArgumentException("Address, in directory and out directory must be specified.");

        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
            return;
        }

        HazelcastInstance client = getHazelcastInstance();
        final JobTracker tracker = client.getJobTracker("default");
        KeyValueSource<String, Tree> sourceTrees = KeyValueSource.fromList(
                client.getList(HazelcastManager.getTreeNamespace()));

        List<String> result = new LinkedList<>();
        String headers = "neighbourhood;trees";
        CsvManager.writeToCSV(getArguments(ClientArgsNames.CSV_OUTPATH), result, headers);
    }

}
