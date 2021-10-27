package ar.edu.itba.pod.client;

import ar.edu.itba.pod.api.Tree;
import ar.edu.itba.pod.api.combiners.Query1CombinerFactory;
import ar.edu.itba.pod.api.mappers.Query1Mapper;
import ar.edu.itba.pod.api.reducers.SumReducerFactory;
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

    private static final int SUCCESS = 0;

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        parseArguments();

        try {
            if (commonArgsNull())
               throw new IllegalArgumentException("Address, in directory and out directory must be specified.");

        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
            return;
        }

        System.out.println("Antes de todo");
        HazelcastInstance client = getHazelcastInstance();
        final JobTracker tracker = client.getJobTracker("query1");

        IList<Tree> trees = client.getList(HazelcastManager.getTreeNamespace());
        System.out.println("trees: " + trees.size());
        KeyValueSource<String, Tree> sourceTrees = KeyValueSource.fromList(trees);
        System.out.println("string de source: " + sourceTrees.toString());
        Job<String, Tree> job = tracker.newJob(sourceTrees);
        System.out.println("string de job: " + job.toString());
        ICompletableFuture<Map<String, Long>> future = job
                .mapper(new Query1Mapper())
                .combiner(new Query1CombinerFactory())
                .reducer(new SumReducerFactory())
                .submit();

        Map<String, Long> rawResult = future.get();
        System.out.println("raw results: " + rawResult.size());
        List<String> outLines = postProcess(rawResult);
        System.out.println("lineas finales: " + outLines.size());

        String headers = "neighbourhood;trees";
        CsvManager.writeToCSV(getArguments(ClientArgsNames.CSV_OUTPATH), outLines, headers);
        System.exit(SUCCESS);
    }

    private static List<String> postProcess(Map<String, Long> rawResult) {
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
        System.out.println("printeando neighs en preProcess");
        trees.forEach(tree -> {
            if(tree.getNeighborhood().getPopulation() == 0)
                trees.remove(tree);
        });
        return trees;
    }
}
