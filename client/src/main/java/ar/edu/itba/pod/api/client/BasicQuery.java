package ar.edu.itba.pod.api.client;

import com.hazelcast.core.HazelcastInstance;
import java.util.HashMap;

public abstract class BasicQuery {

    private final static HashMap<ClientArgsNames, String> arguments = new HashMap<>();

    public static void parseArguments(){
        arguments.put(ClientArgsNames.ADDRESSES,System.getProperty(ClientArgsNames.ADDRESSES.getArgumentName()));
        arguments.put(ClientArgsNames.CSV_OUTPATH,System.getProperty(ClientArgsNames.CSV_OUTPATH.getArgumentName()));
        arguments.put(ClientArgsNames.CSV_INPATH,System.getProperty(ClientArgsNames.CSV_INPATH.getArgumentName()));
        arguments.put(ClientArgsNames.CITY,System.getProperty(ClientArgsNames.CITY.getArgumentName()));
        arguments.put(ClientArgsNames.N,System.getProperty(ClientArgsNames.N.getArgumentName()));
        arguments.put(ClientArgsNames.COMMON_NAME,System.getProperty(ClientArgsNames.COMMON_NAME.getArgumentName()));
        arguments.put(ClientArgsNames.NEIGHBOURHOOD,System.getProperty(ClientArgsNames.NEIGHBOURHOOD.getArgumentName()));

    }

    public static String getArguments(ClientArgsNames name) {
        return arguments.get(name);
    }

    public static boolean commonArgsNull(){
        return (getArguments(ClientArgsNames.CSV_OUTPATH) == null
                || getArguments(ClientArgsNames.CSV_INPATH) == null
                || getArguments(ClientArgsNames.CITY) == null
                || getArguments(ClientArgsNames.CSV_OUTPATH) == null);
    }

    public static HazelcastInstance getHazelcastInstance() {
        QueryData data = CsvManager.readCsvData(getArguments(ClientArgsNames.CSV_INPATH), getArguments(ClientArgsNames.CITY));
        return HazelcastManager.instanceClient(getArguments(ClientArgsNames.ADDRESSES), data);
    }

}
