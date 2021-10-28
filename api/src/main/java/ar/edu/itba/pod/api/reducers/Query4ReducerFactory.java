package ar.edu.itba.pod.api.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import ar.edu.itba.pod.api.Pair;

import java.util.ArrayList;


public class Query4ReducerFactory implements ReducerFactory<Integer, Pair<Integer, ArrayList<String>>, ArrayList<String>> {
    @Override
    public Reducer<Pair<Integer, ArrayList<String>>, ArrayList<String>> newReducer(Integer integer) {
        return new Query4Reducer();
    }

    private class Query4Reducer extends Reducer<Pair<Integer, ArrayList<String>>, ArrayList<String>> {

        private ArrayList<String> neighborhoodsForNumber;

        @Override
        public void reduce(Pair<Integer, ArrayList<String>> neighborhoods) {
            neighborhoodsForNumber.addAll(neighborhoods.snd);
        }

        @Override
        public void beginReduce() {
            this.neighborhoodsForNumber = new ArrayList<>();
        }

        @Override
        public ArrayList<String> finalizeReduce() {
            return this.neighborhoodsForNumber;
        }
    }
}
