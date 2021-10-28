package ar.edu.itba.pod.client;

import ar.edu.itba.pod.api.Tree;
import ar.edu.itba.pod.api.combiners.Query2CombinerFactory;
import ar.edu.itba.pod.api.mappers.Query2Mapper;
import ar.edu.itba.pod.api.reducers.SumReducerFactoryQuery2;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import ar.edu.itba.pod.api.Pair;

import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;
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
        final IList<Tree> trees = client.getList(HazelcastManager.getTreeNamespace());

        final KeyValueSource<String, Tree> sourceTrees = KeyValueSource.fromList(trees);
        final Job<String, Tree> job = tracker.newJob(sourceTrees);
        final ICompletableFuture<Map<Pair<String, String>, Double>> future = job
                .mapper(new Query2Mapper())
                .combiner(new Query2CombinerFactory())
                .reducer(new SumReducerFactoryQuery2())
                .submit();

        final Map<Pair<String,String>, Double> rawResult = future.get();
        final List<String> outLines = postProcess(rawResult);
        String headers = "NEIGHBOURHOOD;COMMON_NAME;TREES_PER_PEOPLE";
        CsvManager.writeToCSV(getArguments(ClientArgsNames.CSV_OUTPATH), outLines, headers);
    }

    private static List<String> postProcess(Map<Pair<String,String>, Double> rawResult) {
        //TODO: MEJORAR EL CODIGO!!!!!!!!!!
        final Map<String, Map<String, Double>> finalMap = new HashMap<>();
        rawResult.forEach( (k, v) -> {
            Map<String, Double> map = new HashMap<>();
            map.put(k.snd, v);
            finalMap.put(k.fst, map);
        });

        List<String> orderKeys = finalMap.keySet().stream().sorted().collect(Collectors.toList());


        return orderKeys.stream()
                .map(entry -> {
                    String treeName = (String) finalMap.get(entry).keySet().toArray()[0];
                    Double value = finalMap.get(entry).get(treeName);
                    DecimalFormat f = new DecimalFormat("##.00");

                    return entry + ";" + treeName + ";" + f.format(value);
                })
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
