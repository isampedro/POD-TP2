package ar.edu.itba.pod.api.client;

import java.util.LinkedList;
import java.util.List;

public class Query5 extends BasicQuery{
    public static void main(String[] args) {

        parseArguments();
        try {
            if (commonArgsNull() && getArguments(ClientArgsNames.COMMON_NAME) == null && getArguments(ClientArgsNames.NEIGHBOURHOOD) == null)
                throw new IllegalArgumentException("Address, in directory and out directory must be specified.");

        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
            return;
        }

        List<String> result = new LinkedList<>();
        String headers = ;
        writeToCSV(getArguments(ClientArgsNames.CSV_OUTPATH), result, headers);
    }
}
