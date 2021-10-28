package ar.edu.itba.pod.client;

import ar.edu.itba.pod.api.Tree;
import ar.edu.itba.pod.api.combiners.Query3CombinerFactory;
import ar.edu.itba.pod.api.mappers.Query3Mapper;
import ar.edu.itba.pod.api.reducers.Query3ReducerFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.math.NumberUtils;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class Query3 extends BasicQuery {
    private static final int SUCCESS = 0, FAILURE = 1;
    private final static Logger logger = LoggerFactory.getLogger(Query3.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        logger.info("Query 3");
        parseArguments();
        logger.info("Argumentos parseados");
        try {
            if (commonArgsNull() || getArguments(ClientArgsNames.N) == null)
                throw new IllegalArgumentException("Address, N, in directory and out directory must be specified.");
            if (!commonArgsOK()) {
                throw new IllegalArgumentException("City, inPath and outPath must be correctly spelled.");
            }
            if (!NumberUtils.isCreatable(getArguments(ClientArgsNames.N))) {
                throw new IllegalArgumentException("N must be a number.");
            }

        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
            System.exit(FAILURE);
        }

        logger.info("Consiguiendo instancia de hazelcast");
        HazelcastInstance client = getHazelcastInstance();
        final JobTracker tracker = client.getJobTracker("query3");

        // We get all the trees and neighbourhoods
        final IList<Tree> trees = client.getList(HazelcastManager.getTreeNamespace());

        final KeyValueSource<String, Tree> sourceTrees = KeyValueSource.fromList(trees);
        final Job<String, Tree> job = tracker.newJob(sourceTrees);
        final ICompletableFuture<Map<String, Integer>> future = job.mapper(new Query3Mapper())
                .combiner(new Query3CombinerFactory()).reducer(new Query3ReducerFactory()).submit();

        final Map<String, Integer> rawResult = future.get();
        final List<String> outLines = postProcess(rawResult, Integer.parseInt(getArguments(ClientArgsNames.N)));
        logger.info("Lineas finales: " + outLines.size());
        String headers = "NEIGHBOURHOOD;COMMON_NAME_COUNT";
        CsvManager.writeToCSV(getArguments(ClientArgsNames.CSV_OUTPATH), outLines, headers);
        trees.clear();
        logger.info("Finalizado con éxito");
        System.exit(SUCCESS);
    }

    private static List<String> postProcess(Map<String, Integer> rawResult, int n) {

        Map<String, Integer> result = sortByValue(rawResult);
        List<String> l = new ArrayList<>(result.keySet());

        return l.stream().map(entry -> entry + ";" + result.get(entry)).collect(Collectors.toList()).subList(0, n);
    }
}
