package ar.edu.itba.pod.api.client;

import ar.edu.itba.pod.api.Tree;
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

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Query4 extends BasicQuery{
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        parseArguments();
        try {
            if (commonArgsNull())
                throw new IllegalArgumentException("Address, in directory and out directory must be specified.");

        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
            return;
        }
        /*
         * pares de barrios con mismo centenar de especies -> salgo de
         * query 3 con <barrio, cantidad de especies diferentes>
         * mapper:tengo que <centenar, barrio>
         * combiner: Map<centenar, list barrio>
         * reducer: quiero terminar map centenar list de barrio. -> concateno combiner
         * post: tengo que combinar los mapas -> se puede hacer en un collator?
         *  mapa final -> para cada mapa, si la key esta, agrego los barrios, sino put
         * devuelvo final
         */

        HazelcastInstance client = getHazelcastInstance();
        final JobTracker tracker = client.getJobTracker("query2");

        // We get all the trees and neighbourhoods
        //TODO: llamar query3
        Map<String, Integer> barrioEspeciesDistintas;
        IMap<String, Integer> IbarrioEspeciesDistintas = client.getMap("trees_species_per_neighborhood");
        IbarrioEspeciesDistintas.putAll(barrioEspeciesDistintas);

        final KeyValueSource<String, Integer> sourceSpeciesPerNeighborhood = KeyValueSource.fromMap(IbarrioEspeciesDistintas);
        final Job<String, Integer> job = tracker.newJob(sourceSpeciesPerNeighborhood);
        final ICompletableFuture<Map<Integer, List<String>>> future = job
                .mapper(new Query4Mapper())
                .combiner(new Query4CombinerFactory())
                .reducer(new Query4ReducerFactory())
                .submit();

        final Map<Integer, List<String>> rawResult = future.get();
        final List<String> outLines = postProcess(rawResult);
        String headers = "NEIGHBOURHOOD;COMMON_NAME_COUNT";
        CsvManager.writeToCSV(getArguments(ClientArgsNames.CSV_OUTPATH), outLines, headers);
    }

    private static IList<Tree> preProcessTrees(IList<Tree> trees) {
        // que solo lleguen aquellos arboles que tienen barrio listado en barrios a los mapper
        trees.forEach(tree -> {
            if(tree.getNeighborhood().getPopulation() == 0)
                trees.remove(tree);
        });
        return trees;
    }

    private static List<String> postProcess(Map<Integer, List<String>> rawResult) {
        final List<String> neighborhoodPairs = new LinkedList<>();
        rawResult.forEach((hundred, neighborhoods) -> {
            for(int i = 0; i < neighborhoods.size(); i++) {
                for(int j = i + 1; j < neighborhoods.size(); j++) {
                    neighborhoodPairs.add(neighborhoods.get(i) + ";" + neighborhoods.get(j));
                }
            }
        });

        return neighborhoodPairs.stream()
                .sorted()
                .collect(Collectors.toList());
    }
}
