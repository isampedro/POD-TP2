package ar.edu.itba.pod.api.mappers;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class Query5MapperB implements Mapper<String, Long, Integer, String>  {
    @Override
    public void map(String street, Long treesForSpecie, Context<Integer, String> context) {
        Integer ten = treesForSpecie.intValue() - (treesForSpecie.intValue() % 10);
        context.emit(ten, street);
    }
}
