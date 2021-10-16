package ar.edu.itba.pod.api.client;

public enum ClientArgsNames {
        ADDRESS ("addresses"),
        CITY ("city"),
        NEIGHBOURHOOD ( "neighbourhood"),
        COMMON_NAME ("commonName"),
        N ("n"),
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
