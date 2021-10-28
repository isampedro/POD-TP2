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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class Query2 extends BasicQuery {
    private static final int SUCCESS = 0, FAILURE = 1;
    private final static Logger logger = LoggerFactory.getLogger(Query2.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        logger.info("Query 2");
        parseArguments();
        logger.info("Argumentos parseados");
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

        logger.info("Consiguiendo instancia de hazelcast");
        HazelcastInstance client = getHazelcastInstance();
        final JobTracker tracker = client.getJobTracker("query2");

        // We get all the trees and neighbourhoods
        final IList<Tree> trees = client.getList(HazelcastManager.getTreeNamespace());
        logger.info("Arboles: " + trees.size());
        final KeyValueSource<String, Tree> sourceTrees = KeyValueSource.fromList(trees);
        logger.info("String de source: " + sourceTrees.toString());
        final Job<String, Tree> job = tracker.newJob(sourceTrees);
        logger.info("String de job: " + job.toString());
        final ICompletableFuture<Map<Pair<String, String>, Double>> future = job.mapper(new Query2Mapper())
                .combiner(new Query2CombinerFactory()).reducer(new SumReducerFactoryQuery2()).submit();

        final Map<Pair<String, String>, Double> rawResult = future.get();
        final List<String> outLines = postProcess(rawResult);
        logger.info("Lineas finales: " + outLines.size());
        String headers = "NEIGHBOURHOOD;COMMON_NAME;TREES_PER_PEOPLE";
        CsvManager.writeToCSV(getArguments(ClientArgsNames.CSV_OUTPATH), outLines, headers);
        System.exit(SUCCESS);
        logger.info("Finalizado con éxito");
    }

    private static List<String> postProcess(Map<Pair<String, String>, Double> rawResult) {
        final Map<String, SortedSet<Pair<Double, String>>> finalMap = new HashMap<>();
        rawResult.forEach((k, v) -> {
            finalMap.putIfAbsent(k.fst, new TreeSet<>(Comparator.reverseOrder()));
            finalMap.get(k.fst).add(new Pair<>(v, k.snd));
        });

        List<String> orderKeys = finalMap.keySet().stream().sorted().collect(Collectors.toList());

        return orderKeys.stream().map(entry -> {
            String treeName = finalMap.get(entry).first().snd;
            Double value = finalMap.get(entry).first().fst;
            DecimalFormat f = new DecimalFormat("0.00", DecimalFormatSymbols.getInstance(Locale.US));

            return entry + ";" + treeName + ";" + f.format(value);
        }).collect(Collectors.toList());
    }
}
