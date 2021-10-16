package ar.edu.itba.pod.api.client;

public enum ClientArgsNames {
        SERVER_ADDRESS ("serverAddress"),
        ACTION_NAME ("action"),
        CATEGORY_NAME ("category"),
        LANE_NAME ("runway"),
        AIRLINE ("airline"),
        FLIGHT_ID ("flightCode"),
        CSV_INPATH ("inPath"),
        CSV_OUTPATH ("outPath");

        String argumentName;

        ClientArgsNames(String argumentName) {
            this.argumentName = argumentName;
        }

        public String getArgumentName() {
            return argumentName;
        }
}
