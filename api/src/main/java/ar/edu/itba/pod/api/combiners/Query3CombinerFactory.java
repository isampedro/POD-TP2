package ar.edu.itba.pod.api.combiners;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

import java.util.HashSet;
import java.util.Set;

public class Query3CombinerFactory implements CombinerFactory<String, String, Set<String>> {
    @Override
    public Combiner<String, Set<String>> newCombiner(String s) {
        return new Query3Combiner();
    }

    private class Query3Combiner extends Combiner<String, Set<String>> {

        private Set<String> distinctSpecies = new HashSet<>();

        @Override
        public void combine(String specie) {
            distinctSpecies.add(specie);
        }

        @Override
        public void reset() {
            this.distinctSpecies.clear();
        }

        @Override
        public Set<String> finalizeChunk() {
            return new HashSet<>(distinctSpecies);
        }
    }
}
