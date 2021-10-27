package ar.edu.itba.pod.api.combiners;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;
import ar.edu.itba.pod.api.Pair;

import java.util.LinkedList;
import java.util.List;

public class Query4CombinerFactory implements CombinerFactory<Integer, String, Pair<Integer, List<String>>> {

    @Override
    public Combiner<String, Pair<Integer, List<String>>> newCombiner(Integer hundred) {
        return new Query4Combiner(hundred);
    }

    private class Query4Combiner extends Combiner<String, Pair<Integer, List<String>>> {

        private final List<String> neighborhoods;
        private final Integer number;

        public Query4Combiner(Integer number) {
            this.number = number;
            this.neighborhoods = new LinkedList<>();
        }

        @Override
        public void combine(String neighborhood) {
            neighborhoods.add(neighborhood);
        }

        @Override
        public void reset() {
            this.neighborhoods.clear();
        }

        @Override
        public Pair<Integer, List<String>> finalizeChunk() {
            return new Pair<>(this.number, this.neighborhoods);
        }
    }
}
