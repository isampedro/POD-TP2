package ar.edu.itba.pod.api.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import com.sun.tools.javac.util.Pair;

public class SumReducerFactoryQuery5 implements ReducerFactory<Pair<String,String>, Long, Long> {
    @Override
    public Reducer<Long, Long> newReducer(Pair<String,String> s) {
        return new SumReducer();
    }

    private class SumReducer extends Reducer<Long, Long> {
        private long sum;

        @Override
        public void beginReduce() {
            sum = 0;
        }

        @Override
        public void reduce(Long value) {
            sum += value;
        }

        @Override
        public Long finalizeReduce() {
            return sum;
        }
    }
}