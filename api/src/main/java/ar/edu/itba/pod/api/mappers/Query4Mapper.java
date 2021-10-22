package ar.edu.itba.pod.api.mappers;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class Query4Mapper implements Mapper<String, Integer, Integer, String> {
    @Override
    public void map(String neighborhood, Integer distinctSpecies, Context<Integer, String> context) {
        int hundred = distinctSpecies - (distinctSpecies % 100);
        context.emit(hundred, neighborhood);
    }
}
