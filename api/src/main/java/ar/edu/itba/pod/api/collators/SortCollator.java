package ar.edu.itba.pod.api.collators;

import com.hazelcast.mapreduce.Collator;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SortCollator<K2, V4> implements Collator<Map.Entry<K2, V4>, List<String>> {

    @Override
    public List<String> collate(Iterable<Map.Entry<K2, V4>> iterable) {
        final List<Map.Entry<K2, V4>> entries = new LinkedList<>();
        iterable.iterator().forEachRemaining(entries::add);

        List<Map.Entry<K2, V4>> result = entries.stream()
                .sorted(Comparator.comparing((Function<Map.Entry<K2, V4>, V4>) Map.Entry::getValue)
                        .thenComparing(Map.Entry::getKey)).collect(Collectors.toList());

        return result.stream()
                .map(entry -> entry.getKey() + ";" + entry.getValue())
                .collect(Collectors.toList());
    }
}
