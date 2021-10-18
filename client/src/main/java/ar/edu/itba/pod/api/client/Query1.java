package ar.edu.itba.pod.api.client;

import ar.edu.itba.pod.api.Neighborhood;
import ar.edu.itba.pod.api.Tree;
import ar.edu.itba.pod.api.combiners.Query1CombinerFactory;
import ar.edu.itba.pod.api.mappers.Query1Mapper;
import ar.edu.itba.pod.api.reducers.Query1ReducerFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Query1 extends BasicQuery{

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        parseArguments();

        try {
            if (commonArgsNull())
               throw new IllegalArgumentException("Address, in directory and out directory must be specified.");

        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
            return;
        }

        HazelcastInstance client = getHazelcastInstance();
        final JobTracker tracker = client.getJobTracker("query1");
        IList<Tree> trees = preProcess(client.getList(HazelcastManager.getTreeNamespace()));
        KeyValueSource<String, Tree> sourceTrees = KeyValueSource.fromList(trees);
        Job<String, Tree> job = tracker.newJob(sourceTrees);
        ICompletableFuture<Map<String, Long>> future = job
                .mapper(new Query1Mapper())
                .combiner(new Query1CombinerFactory())
                .reducer(new Query1ReducerFactory())
                .submit();

        Map<String, Long> rawResult = future.get();
        List<String> outLines = postProcess(rawResult, client);

        String headers = "neighbourhood;trees";
        CsvManager.writeToCSV(getArguments(ClientArgsNames.CSV_OUTPATH), outLines, headers);
    }

    private static List<String> postProcess(Map<String, Long> rawResult, HazelcastInstance client) {
        //IList<Neighborhood> neighborhoods = client.getList(HazelcastManager.getNeighborhoodNamespace());
        //final List<String> neighborhoodsNames = neighborhoods.stream()
        //        .map(Neighborhood::getName).collect(Collectors.toList());

        /*List<Map.Entry<String, Long>> result = rawResult.entrySet().stream()
                .filter(entry -> neighborhoodsNames.contains(entry.getKey()))
                .sorted(Comparator.comparing((Function<Map.Entry<String, Long>, Long>) Map.Entry::getValue)
                        .thenComparing(Map.Entry::getKey)).collect(Collectors.toList()); */

        List<Map.Entry<String, Long>> result = rawResult.entrySet().stream()
                .sorted(Comparator.comparing((Function<Map.Entry<String, Long>, Long>) Map.Entry::getValue)
                .thenComparing(Map.Entry::getKey)).collect(Collectors.toList());

        return result.stream()
                .map(entry -> entry.getKey() + ";" + entry.getValue())
                .collect(Collectors.toList());
    }

    private static IList<Tree> preProcess(IList<Tree> trees) {
        // que solo lleguen aquellos arboles que tienen barrio listado en barrios a los mapper
        trees.forEach(tree -> {
            if(tree.getNeighborhood().getPopulation() == 0)
                trees.remove(tree);
        });
        return trees;
    }
}
