package ar.edu.itba.pod.api.client;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

public abstract class BasicQuery {

    private final static HashMap<ClientArgsNames, String> arguments = new HashMap<>();

    public static void parseArguments(){
        arguments.put(ClientArgsNames.ADDRESS,System.getProperty(ClientArgsNames.ADDRESS.getArgumentName()));
        arguments.put(ClientArgsNames.CSV_OUTPATH,System.getProperty(ClientArgsNames.CSV_OUTPATH.getArgumentName()));
        arguments.put(ClientArgsNames.CSV_INPATH,System.getProperty(ClientArgsNames.CSV_INPATH.getArgumentName()));
        arguments.put(ClientArgsNames.CITY,System.getProperty(ClientArgsNames.CITY.getArgumentName()));
        arguments.put(ClientArgsNames.N,System.getProperty(ClientArgsNames.N.getArgumentName()));
        arguments.put(ClientArgsNames.COMMON_NAME,System.getProperty(ClientArgsNames.COMMON_NAME.getArgumentName()));
        arguments.put(ClientArgsNames.NEIGHBOURHOOD,System.getProperty(ClientArgsNames.NEIGHBOURHOOD.getArgumentName()));

    }

    public static void writeToCSV(String outPath, List<String> results, String headers){
        results.add(0, headers);
        try(BufferedWriter bw = new BufferedWriter(new FileWriter(outPath))) {
            for (String result : results) {
                bw.write(result);
                bw.newLine();
            }
        } catch (IOException e) {
            //logger.error("IOException {} ",e.getMessage());
        }
    }

    public static String getArguments(ClientArgsNames name) {
        return arguments.get(name);
    }
    public static boolean commonArgsNull(){
        return (getArguments(ClientArgsNames.CSV_OUTPATH) == null || getArguments(ClientArgsNames.CSV_INPATH) == null ||getArguments(ClientArgsNames.CITY) == null || getArguments(ClientArgsNames.CSV_OUTPATH) == null);
    }

}
