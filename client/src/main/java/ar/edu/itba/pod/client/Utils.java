package ar.edu.itba.pod.client;

import java.util.*;

public class Utils <K extends Comparable<K>, V extends Comparable<V>>{

    public static <K extends Comparable<K> ,V extends Comparable<V>> Map<K ,V> sortByValue(Map<K, V> hm) {
        // Create a list from elements of HashMap
        List<Map.Entry<K, V>> list = new LinkedList<>(hm.entrySet());

        // Sort the list
        list.sort(new Comparator<Map.Entry<K, V>>() {
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                int compareSpecie = o2.getValue().compareTo(o1.getValue());
                int compareName = o1.getKey().compareTo(o2.getKey());
                return compareSpecie == 0 ? compareName : compareSpecie;
            }
        });

        // put data from sorted list to hashmap
        HashMap<K, V> temp = new LinkedHashMap<>();
        for (Map.Entry<K, V> aa : list) {
            temp.put(aa.getKey(), aa.getValue());
        }
        return temp;
    }

}
