package ar.edu.itba.pod.client;

import com.hazelcast.core.HazelcastInstance;
import org.slf4j.Logger;
import java.io.File;
import java.util.HashMap;

public abstract class BasicQuery {

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

    public static HazelcastInstance getHazelcastInstance(Logger logger) {
        logger.info("CSV processing started");
        QueryData data = CsvManager.readCsvData(getArguments(ClientArgsNames.CSV_INPATH),
                getArguments(ClientArgsNames.CITY));
        logger.info("CSV processing finished");
        logger.info("Data load started");
        return HazelcastManager.instanceClient(getArguments(ClientArgsNames.ADDRESSES), data);
    }

}
