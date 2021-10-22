package ar.edu.itba.pod.api.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.HashSet;
import java.util.Set;

public class Query3ReducerFactory implements ReducerFactory<String, Set<String>, Integer> {
    @Override
    public Reducer<Set<String>, Integer> newReducer(String neighborhood) {
        return new Query3Reducer();
    }

    private class Query3Reducer extends Reducer<Set<String>, Integer> {

        private Set<String> distinctSpecies;

        @Override
        public void beginReduce() {
            distinctSpecies = new HashSet<>();
        }

        @Override
        public void reduce(Set<String> species) {
            distinctSpecies.addAll(species);
        }

        @Override
        public Integer finalizeReduce() {
            return distinctSpecies.size();
        }
    }
}
