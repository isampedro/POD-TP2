package ar.edu.itba.pod.client;

import ar.edu.itba.pod.api.Tree;
import ar.edu.itba.pod.api.collators.Query1Collator;
import ar.edu.itba.pod.api.combiners.LongSumCombiner;
import ar.edu.itba.pod.api.mappers.Query1Mapper;
import ar.edu.itba.pod.api.reducers.LongSumReducerFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class Query1 extends BasicQuery {

    private static final int SUCCESS = 0, FAILURE = 1;
    private final static Logger logger = LoggerFactory.getLogger(Query1.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        parseArguments();
        try {
            if (commonArgsNull())
                throw new IllegalArgumentException("Address, in directory and out directory must be specified.");
            if (!commonArgsOK()) {
                throw new IllegalArgumentException("City, inPath and outPath must be correctly spelled.");
            }

        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
            System.exit(FAILURE);
        }

        HazelcastInstance client = getHazelcastInstance(logger);
        logger.info("Data load finished");
        final JobTracker tracker = client.getJobTracker("query1");

        IList<Tree> trees = client.getList(HazelcastManager.getTreeNamespace());
        logger.info("Data retrieved");

        KeyValueSource<String, Tree> sourceTrees = KeyValueSource.fromList(trees);
        Job<String, Tree> job = tracker.newJob(sourceTrees);
        logger.info("MapReduce Started");
        ICompletableFuture<List<String>> future = job.mapper(new Query1Mapper())
                .combiner(new LongSumCombiner()).reducer(new LongSumReducerFactory())
                .submit(new Query1Collator<>());
        logger.info("MapReduce Finished");
        List<String> rawResult = future.get();
        //List<String> outLines = postProcess(rawResult);

        String headers = "neighbourhood;trees";
        CsvManager.writeToCSV(getArguments(ClientArgsNames.CSV_OUTPATH), rawResult, headers);
        trees.clear();
        System.exit(SUCCESS);
    }

    private static List<String> postProcess(Map<String, Long> rawResult) {

        Map<String, Long> result = sortByValue(rawResult);
        List<String> l = new ArrayList<>(result.keySet());

        return l.stream().map(entry -> entry + ";" + result.get(entry)).collect(Collectors.toList());
    }

}
