package ar.edu.itba.pod.api.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import ar.edu.itba.pod.api.Pair;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

public class Query4ReducerFactory implements ReducerFactory<Integer, Pair<Integer, List<String>>, List<String>> {
    @Override
    public Reducer<Pair<Integer, List<String>>, List<String>> newReducer(Integer integer) {
        return new Query4Reducer();
    }

    private class Query4Reducer extends Reducer<Pair<Integer, List<String>>, List<String>> {

        private List<String> neighborhoodsForNumber;

        @Override
        public void reduce(Pair<Integer, List<String>> neighborhoods) {
            neighborhoodsForNumber.addAll(neighborhoods.snd);
        }

        @Override
        public void beginReduce() {
            this.neighborhoodsForNumber = new ArrayList<>();
        }

        @Override
        public List<String> finalizeReduce() {
            return this.neighborhoodsForNumber;
        }
    }
}
