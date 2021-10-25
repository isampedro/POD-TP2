package ar.edu.itba.pod.api.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import ar.edu.itba.pod.api.Pair;

public class AverageReducerFactory implements ReducerFactory<Pair, Double, Double> {
    @Override
    public Reducer<Double, Double> newReducer(Pair p) {
        return new AverageReducer();
    }

    private class AverageReducer extends Reducer<Double, Double> {
        private double sum;
        private long amount;

        @Override
        public void beginReduce() {
            sum = 0;
            amount = 0;
        }

        @Override
        public void reduce(Double value) {
            sum += value;
            amount++;
        }

        @Override
        public Double finalizeReduce() {
            return sum/amount;
        }
    }
}
