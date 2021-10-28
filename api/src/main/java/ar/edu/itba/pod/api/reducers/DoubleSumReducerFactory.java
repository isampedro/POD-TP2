package ar.edu.itba.pod.api.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import ar.edu.itba.pod.api.Pair;

public class DoubleSumReducerFactory implements ReducerFactory<Pair<String,String>, Double, Double> {
    @Override
    public Reducer<Double, Double> newReducer(Pair<String,String> s) {
        return new SumReducer();
    }

    private class SumReducer extends Reducer<Double, Double> {
        private double sum;

        @Override
        public void beginReduce() {
            sum = 0;
        }

        @Override
        public void reduce(Double value) {
            sum += value;
        }

        @Override
        public Double finalizeReduce() {
            return sum;
        }
    }
}
