package ar.edu.itba.pod.api.mappers;

import ar.edu.itba.pod.api.Tree;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class Query1Mapper implements Mapper<String, Tree, String, Long> {
    private static final long serialVersionUID = 1L;

    @Override
    public void map(String key, Tree value, Context<String, Long> context) {
        context.emit(value.getNeighborhood().getName(), 1L);
    }
}
