package ar.edu.itba.pod.api.mappers;

import ar.edu.itba.pod.api.Tree;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class Query5Mapper implements Mapper<String, Tree, String, Long> {
    private static final long serialVersionUID = 1L;
    final private String specie;
    final private String neighbour;

    public Query5Mapper(String speciesName, String neighbour) {
        this.specie = speciesName;
        this.neighbour = neighbour;
    }

    @Override
    public void map(String key, Tree value, Context<String, Long> context) {
        if (value.getName().equals(specie) && value.getNeighborhood().getName().equals(neighbour))
            context.emit(value.getStreet(), 1L);
    }
}
