package ar.edu.itba.pod.api.client;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class Query1 {

    private static String city;
    private static String address;
    private static String inPath;
    private static String outPath;

    public static void main(String[] args) {

        //logger.info("Query client starting...");

        try {
            address = System.getProperty(ClientArgsNames.SERVER_ADDRESS.getArgumentName());
            inPath = System.getProperty(ClientArgsNames.CSV_OUTPATH.getArgumentName());
            outPath = System.getProperty(ClientArgsNames.LANE_NAME.getArgumentName());
            city = System.getProperty(ClientArgsNames.AIRLINE.getArgumentName());

            if(outPath == null || address == null || inPath == null || city == null) {
                throw new IllegalArgumentException("Address, in directory and out directory must be specified.");
            }

        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
        }

        List<String> result = new LinkedList<>();
        writeToCSV(outPath, result);
    }

    private static void writeToCSV(String outPath, List<String> results){
        results.add(0, "Neighbourhood;Trees");
        try(BufferedWriter bw = new BufferedWriter(new FileWriter(outPath))) {
            for (String result : results) {
                bw.write(result);
                bw.newLine();
            }
        } catch (IOException e) {
            //logger.error("IOException {} ",e.getMessage());
        }
    }
}
