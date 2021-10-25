package ar.edu.itba.pod.api.client;

import ar.edu.itba.pod.api.Tree;
import ar.edu.itba.pod.api.combiners.Query1CombinerFactory;
import ar.edu.itba.pod.api.combiners.Query5CombinerFactory;
import ar.edu.itba.pod.api.mappers.Query1Mapper;
import ar.edu.itba.pod.api.mappers.Query5Mapper;
import ar.edu.itba.pod.api.reducers.SumReducerFactory;
import ar.edu.itba.pod.api.reducers.SumReducerFactoryQuery5;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import ar.edu.itba.pod.api.Pair;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Query5 extends BasicQuery{
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        parseArguments();

        try {
            if (commonArgsNull() || getArguments(ClientArgsNames.COMMON_NAME) == null || getArguments(ClientArgsNames.NEIGHBOURHOOD) == null)
                throw new IllegalArgumentException("Address, in directory and out directory must be specified.");

        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
            return;
        }

        HazelcastInstance client = getHazelcastInstance();
        final JobTracker tracker = client.getJobTracker("query1");
        IList<Tree> trees = preProcessTrees(client.getList(HazelcastManager.getTreeNamespace()));
        KeyValueSource<String, Tree> sourceTrees = KeyValueSource.fromList(trees);
        Job<String, Tree> job = tracker.newJob(sourceTrees);
        ICompletableFuture<Map<Pair<String,String>, Long>> future = job
                .mapper(new Query5Mapper())
                .combiner(new Query5CombinerFactory())
                .reducer(new SumReducerFactoryQuery5())
                .submit();

        Map<Pair<String, String>, Long> rawResult = future.get();
        List<String> outLines = postProcess(rawResult, getArguments(ClientArgsNames.NEIGHBOURHOOD), getArguments(ClientArgsNames.COMMON_NAME));

        String headers = "GROUP;STREET A; STREET B";
        CsvManager.writeToCSV(getArguments(ClientArgsNames.CSV_OUTPATH), outLines, headers);
    }

    private static List<String> postProcess( final Map<Pair<String, String>, Long> rawResult, String neighbourhood, String commonName ) {
        final Map<Pair<String,String>, Long> filteredRawResult = rawResult.entrySet().stream()
                        .filter( x -> x.getKey().fst.equals(neighbourhood))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        final Map<String, String> streetPairs = new HashMap<>();
        filteredRawResult.forEach( (k, v) -> {
            filteredRawResult.forEach( (k1, v1) -> {
                if( !k.snd.equals(k1.snd) && v.equals(v1) ) {
                    if( !streetPairs.containsKey(k.snd) && !(streetPairs.containsKey(k1.fst) && !streetPairs.get(k1.fst).contains(k.snd)) )
                        streetPairs.put(k.snd, k1.snd);
                }
            });
        });
        List<Map.Entry<String, Integer>> result = filteredRawResult.entrySet().stream()
                                                    .sorted(Comparator.comparing())
        List<Map.Entry<String, Integer>> result = rawResult.entrySet().stream()
                .sorted(Comparator.comparing((Function<Map.Entry<Pair<String,String>, Long>, Integer>) Map.Entry::getValue)
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
