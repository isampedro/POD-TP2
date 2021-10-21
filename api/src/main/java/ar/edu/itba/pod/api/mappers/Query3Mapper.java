package ar.edu.itba.pod.api.mappers;

import ar.edu.itba.pod.api.Tree;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class Query3Mapper implements Mapper<String, Tree, String, String>  {
    private static final long serialVersionUID = 1L;

    @Override
    public void map(String key, Tree value, Context<String, String> context) {
        context.emit(value.getNeighborhood().getName(), value.getName());
    }
}
