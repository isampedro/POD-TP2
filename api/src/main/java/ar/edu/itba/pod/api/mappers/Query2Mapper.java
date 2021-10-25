package ar.edu.itba.pod.api.mappers;

import ar.edu.itba.pod.api.Neighborhood;
import ar.edu.itba.pod.api.Tree;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import ar.edu.itba.pod.api.Pair;

public class Query2Mapper implements Mapper<String, Tree, Pair<String, String>, Double> {
    private static final double serialVersionUID = 1D;

    @Override
    public void map(String s, Tree tree, Context<Pair<String, String>, Double> context) {
        context.emit(new Pair<>(tree.getNeighborhood().getName(), tree.getName()), 1/(double)tree.getNeighborhood().getPopulation());
    }
}