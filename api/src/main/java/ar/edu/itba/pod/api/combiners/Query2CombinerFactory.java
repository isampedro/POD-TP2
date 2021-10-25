package ar.edu.itba.pod.api.combiners;

import ar.edu.itba.pod.api.Neighborhood;
import ar.edu.itba.pod.api.Tree;
import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;
import ar.edu.itba.pod.api.Pair;

public class Query2CombinerFactory implements CombinerFactory<Pair<String, String>, Double, Double> {
    @Override
    public Combiner<Double, Double> newCombiner(Pair<String, String> neighborhoodTreePair) {
        return new Query2Combiner();
    }

    private class Query2Combiner extends Combiner<Double, Double> {
        private double sum = 0;

        @Override
        public void combine(Double value) {
            sum += value;
        }

        @Override
        public Double finalizeChunk() {
            return sum;
        }

        @Override
        public void reset() {
            sum = 0.0;
        }
    }
}
