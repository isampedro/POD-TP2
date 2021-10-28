package ar.edu.itba.pod.api.combiners;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;
import ar.edu.itba.pod.api.Pair;

import java.util.ArrayList;

public class Query4CombinerFactory implements CombinerFactory<Integer, String, Pair<Integer, ArrayList<String>>> {

    @Override
    public Combiner<String, Pair<Integer, ArrayList<String>>> newCombiner(Integer hundred) {
        return new Query4Combiner(hundred);
    }

    private class Query4Combiner extends Combiner<String, Pair<Integer, ArrayList<String>>> {

        private final ArrayList<String> neighborhoods;
        private final Integer number;

        public Query4Combiner(Integer number) {
            this.number = number;
            this.neighborhoods = new ArrayList<>();
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
        public Pair<Integer, ArrayList<String>> finalizeChunk() {
            return new Pair<>(this.number, this.neighborhoods);
        }
    }
}
