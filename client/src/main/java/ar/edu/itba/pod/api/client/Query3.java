package ar.edu.itba.pod.api.client;

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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Query3 extends BasicQuery{
    public static void main(String[] args) throws ExecutionException, InterruptedException {


        parseArguments();
        try {
            if (commonArgsNull() || getArguments(ClientArgsNames.N) == null)
                throw new IllegalArgumentException("Address, in directory and out directory must be specified.");

        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
            return;
        }

    HazelcastInstance client = getHazelcastInstance();
    final JobTracker tracker = client.getJobTracker("query3");

    // We get all the trees and neighbourhoods
    final IList<Tree> trees = preProcessTrees(client.getList(HazelcastManager.getTreeNamespace()));

    final KeyValueSource<String, Tree> sourceTrees = KeyValueSource.fromList(trees);
    final Job<String, Tree> job = tracker.newJob(sourceTrees);
    final ICompletableFuture<Map<String, Integer>> future = job
            .mapper(new Query3Mapper())
            .combiner(new Query3CombinerFactory())
            .reducer(new Query3ReducerFactory())
            .submit();

    final Map<String, Integer> rawResult = future.get();
    final List<String> outLines = postProcess(rawResult, Integer.parseInt(ClientArgsNames.N.getArgumentName()));
    String headers = "NEIGHBOURHOOD;COMMON_NAME_COUNT";
    CsvManager.writeToCSV(getArguments(ClientArgsNames.CSV_OUTPATH), outLines, headers);

    }

    // TODO: que de todo esto se puede pasar a un collator
    private static List<String> postProcess(Map<String, Integer> rawResult, int n) {
        List<Map.Entry<String, Integer>> result = rawResult.entrySet().stream()
                .sorted(Comparator.comparing((Function<Map.Entry<String, Integer>, Integer>) Map.Entry::getValue)
                        .thenComparing(Map.Entry::getKey)).collect(Collectors.toList()).subList(0, n-1);

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
