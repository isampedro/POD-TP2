package ar.edu.itba.pod.api.combiners;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class LongSumCombiner implements CombinerFactory<String, Long, Long> {
    @Override
    public Combiner<Long, Long> newCombiner(String key) {
        return new Query1Combiner();
    }

    private class Query1Combiner extends Combiner<Long, Long> {
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
