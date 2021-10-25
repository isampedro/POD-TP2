package ar.edu.itba.pod.api.client;

import ar.edu.itba.pod.api.Neighborhood;
import ar.edu.itba.pod.api.Tree;
import ar.edu.itba.pod.api.combiners.Query1CombinerFactory;
import ar.edu.itba.pod.api.combiners.Query2CombinerFactory;
import ar.edu.itba.pod.api.mappers.Query1Mapper;
import ar.edu.itba.pod.api.mappers.Query2Mapper;
import ar.edu.itba.pod.api.reducers.AverageReducerFactory;
import ar.edu.itba.pod.api.reducers.SumReducerFactory;
import ar.edu.itba.pod.api.reducers.SumReducerFactoryQuery2;
import com.hazelcast.client.proxy.ClientListProxy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import ar.edu.itba.pod.api.Pair;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Query2 extends BasicQuery{
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
        final JobTracker tracker = client.getJobTracker("query2");

        // We get all the trees and neighbourhoods
        final IList<Tree> trees = preProcessTrees(client.getList(HazelcastManager.getTreeNamespace()));

        final KeyValueSource<String, Tree> sourceTrees = KeyValueSource.fromList(trees);
        final Job<String, Tree> job = tracker.newJob(sourceTrees);
        final ICompletableFuture<Map<Pair<String, String>, Double>> future = job
                .mapper(new Query2Mapper())
                .combiner(new Query2CombinerFactory())
                .reducer(new SumReducerFactoryQuery2())
                .submit();

        final Map<Pair<String,String>, Double> rawResult = future.get();
        final List<String> outLines = postProcess(rawResult, client);
        String headers = "NEIGHBOURHOOD;COMMON_NAME;TREES_PER_PEOPLE";
        CsvManager.writeToCSV(getArguments(ClientArgsNames.CSV_OUTPATH), outLines, headers);
    }

    private static List<String> postProcess(Map<Pair<String,String>, Double> rawResult, HazelcastInstance client) {
        //TODO: MEJORAR EL CODIGO!!!!!!!!!!
        final Map<String, Double> finalMap = new HashMap<>();
        rawResult.forEach( (k, v) -> finalMap.put( k.fst + k.snd, v ));

        List<Map.Entry<String, Double>> result = finalMap.entrySet().stream()
                .sorted(Comparator.comparing((Function<Map.Entry<String, Double>, Double>) Map.Entry::getValue)
                        .thenComparing(Map.Entry::getKey)).collect(Collectors.toList());

        return result.stream()
                .map(entry -> entry.getKey() + ";" + entry.getValue())
                .collect(Collectors.toList());
    }

    private static IList<Tree> preProcessTrees(IList<Tree> trees) {
        // que solo lleguen aquellos arboles que tienen barrio listado en barrios a los mapper
        trees.forEach(tree -> {
            if(tree.getNeighborhood().getPopulation() == 0)
                trees.remove(tree);
        });
        return trees;
    }
}
