package ar.edu.itba.pod.api.combiners;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;
import com.sun.tools.javac.util.Pair;

public class Query5CombinerFactory implements CombinerFactory<Pair<String,String>, Long, Long> {
    @Override
    public Combiner<Long, Long> newCombiner(Pair<String, String> key) {
        return new Query5Combiner();
    }

    private class Query5Combiner extends Combiner<Long, Long> {
        private long sum = 0;

        @Override
        public void combine(Long value) {
            sum += value;
        }

        @Override
        public Long finalizeChunk() {
            return sum;
        }

        @Override
        public void reset() {
            sum = 0;
        }
    }
}
