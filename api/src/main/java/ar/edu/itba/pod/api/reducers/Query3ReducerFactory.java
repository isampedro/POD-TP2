package ar.edu.itba.pod.api.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.HashSet;
import java.util.Set;

public class Query3ReducerFactory implements ReducerFactory<String, String, Integer> {
    @Override
    public Reducer<String, Integer> newReducer(String neighborhood) {
        return new Query3Reducer();
    }

    private class Query3Reducer extends Reducer<String, Integer> {

        private Set<String> species;

        @Override
        public void beginReduce() {
            species = new HashSet<>();
        }

        @Override
        public void reduce(String s) {
            species.add(s);
        }

        @Override
        public Integer finalizeReduce() {
            return species.size();
        }
    }
}
