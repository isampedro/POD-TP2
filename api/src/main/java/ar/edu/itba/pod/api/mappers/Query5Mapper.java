package ar.edu.itba.pod.api.mappers;

import ar.edu.itba.pod.api.Tree;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import com.sun.tools.javac.util.Pair;

public class Query5Mapper implements Mapper<String, Tree, Pair<String,String>, Long> {
    private static final long serialVersionUID = 1L;

    @Override
    public void map(String key, Tree value, Context<Pair<String,String>, Long> context) {
        context.emit(new Pair<>(value.getNeighborhood().getName(), value.getStreet()), 1L);
    }
}
