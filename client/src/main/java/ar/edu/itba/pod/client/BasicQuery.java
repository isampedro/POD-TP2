package ar.edu.itba.pod.client;

import com.hazelcast.core.HazelcastInstance;

import java.io.File;
import java.util.*;

public abstract class BasicQuery <K extends Comparable<K>, V extends Comparable<V>> {

    private final static HashMap<ClientArgsNames, String> arguments = new HashMap<>();

    public static void parseArguments() {
        arguments.put(ClientArgsNames.ADDRESSES, System.getProperty(ClientArgsNames.ADDRESSES.getArgumentName()));
        arguments.put(ClientArgsNames.CSV_OUTPATH, System.getProperty(ClientArgsNames.CSV_OUTPATH.getArgumentName()));
        arguments.put(ClientArgsNames.CSV_INPATH, System.getProperty(ClientArgsNames.CSV_INPATH.getArgumentName()));
        arguments.put(ClientArgsNames.CITY, System.getProperty(ClientArgsNames.CITY.getArgumentName()));
        arguments.put(ClientArgsNames.N, System.getProperty(ClientArgsNames.N.getArgumentName()));
        arguments.put(ClientArgsNames.COMMON_NAME, System.getProperty(ClientArgsNames.COMMON_NAME.getArgumentName()));
        arguments.put(ClientArgsNames.NEIGHBOURHOOD,
                System.getProperty(ClientArgsNames.NEIGHBOURHOOD.getArgumentName()));

    }

    public static String getArguments(ClientArgsNames name) {
        return arguments.get(name);
    }

    public static boolean commonArgsNull() {
        return (getArguments(ClientArgsNames.ADDRESSES) == null || getArguments(ClientArgsNames.CSV_INPATH) == null
                || getArguments(ClientArgsNames.CITY) == null || getArguments(ClientArgsNames.CSV_OUTPATH) == null);
    }

    public static boolean commonArgsOK() {
        File inputFolder = new File(getArguments(ClientArgsNames.CSV_INPATH));
        File[] inputFolderFiles = inputFolder.listFiles();

        boolean inputTreesFound = false, inputNeighborhoodsFound = false;

        for (int i = 0; i < inputFolderFiles.length; i++) {
            if (inputFolderFiles[i].getName()
                    .equals(FileTypes.TREES.getFileType() + getArguments(ClientArgsNames.CITY) + ".csv"))
                inputTreesFound = true;
            if (inputFolderFiles[i].getName()
                    .equals(FileTypes.NEIGHBOURHOODS.getFileType() + getArguments(ClientArgsNames.CITY) + ".csv"))
                inputNeighborhoodsFound = true;
        }

        // addressesMatches = getArguments(ClientArgsNames.ADDRESSES).matches(
        // "^(?:(?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9]).){3}(?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9]):(?:6553[0-5]|655[0-2][0-9]|65[0-4][0-9]{2}|6[0-4][0-9]{3}|[1-5][0-9]{4}|[1-9][0-9]{1,3}|[0-9])");

        return inputNeighborhoodsFound && inputTreesFound;
    }

    public static HazelcastInstance getHazelcastInstance() {
        QueryData data = CsvManager.readCsvData(getArguments(ClientArgsNames.CSV_INPATH),
                getArguments(ClientArgsNames.CITY));
        return HazelcastManager.instanceClient(getArguments(ClientArgsNames.ADDRESSES), data);
    }

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
