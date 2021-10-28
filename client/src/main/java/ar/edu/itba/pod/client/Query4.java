package ar.edu.itba.pod.client;

import ar.edu.itba.pod.api.Tree;
import ar.edu.itba.pod.api.combiners.Query3CombinerFactory;
import ar.edu.itba.pod.api.combiners.Query4CombinerFactory;
import ar.edu.itba.pod.api.mappers.Query3Mapper;
import ar.edu.itba.pod.api.mappers.Query4Mapper;
import ar.edu.itba.pod.api.reducers.Query3ReducerFactory;
import ar.edu.itba.pod.api.reducers.Query4ReducerFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class Query4 extends BasicQuery {
    private static final int SUCCESS = 0, FAILURE = 1;

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
        /*
         * pares de barrios con mismo centenar de especies -> salgo de query 3 con
         * <barrio, cantidad de especies diferentes> mapper:tengo que <centenar, barrio>
         * combiner: Map<centenar, list barrio> reducer: quiero terminar map centenar
         * list de barrio. -> concateno combiner post: tengo que combinar los mapas ->
         * se puede hacer en un collator? mapa final -> para cada mapa, si la key esta,
         * agrego los barrios, sino put devuelvo final
         */

        HazelcastInstance client = getHazelcastInstance();
        final JobTracker tracker = client.getJobTracker("query4");

        final IList<Tree> trees = preProcessTrees(client.getList(HazelcastManager.getTreeNamespace()));

        // Get neighborhoods with their different tree species
        final KeyValueSource<String, Tree> sourceTrees = KeyValueSource.fromList(trees);

        final Job<String, Tree> job = tracker.newJob(sourceTrees);
        final ICompletableFuture<Map<String, Integer>> future = job.mapper(new Query3Mapper())
                .combiner(new Query3CombinerFactory()).reducer(new Query3ReducerFactory()).submit();

        final Map<String, Integer> rawResult = future.get();

        IMap<String, Integer> differentSpecies = client.getMap("trees_species_per_neighborhood");
        differentSpecies.putAll(rawResult);
        final KeyValueSource<String, Integer> sourceSpeciesPerNeighborhood = KeyValueSource.fromMap(differentSpecies);

        final Job<String, Integer> finalJob = tracker.newJob(sourceSpeciesPerNeighborhood);
        final ICompletableFuture<Map<Integer, ArrayList<String>>> finalFuture = finalJob.mapper(new Query4Mapper())
                .combiner(new Query4CombinerFactory()).reducer(new Query4ReducerFactory()).submit();

        final Map<Integer, ArrayList<String>> finalRawResult = finalFuture.get();
        final List<String> outLines = postProcess(finalRawResult);
        String headers = "GROUP;NEIGHBOURHOOD A;NEIGHBOURHOOD B";
        CsvManager.writeToCSV(getArguments(ClientArgsNames.CSV_OUTPATH), outLines, headers);
        System.exit(SUCCESS);
    }

    private static IList<Tree> preProcessTrees(IList<Tree> trees) {
        // que solo lleguen aquellos arboles que tienen barrio listado en barrios a los
        // mapper
        trees.forEach(tree -> {
            if (tree.getNeighborhood().getPopulation() == 0)
                trees.remove(tree);
        });
        return trees;
    }

    private static List<String> postProcess(Map<Integer, ArrayList<String>> rawResult) {
        final List<String> neighborhoodPairs = new LinkedList<>();
        rawResult.forEach((hundred, neighborhoods) -> {
            for (int i = 0; i < neighborhoods.size(); i++) {
                for (int j = i + 1; j < neighborhoods.size(); j++) {
                    neighborhoodPairs.add(hundred + ";" + neighborhoods.get(i) + ";" + neighborhoods.get(j));
                }
            }
        });

        return neighborhoodPairs.stream().sorted().collect(Collectors.toList());
    }
}
