package ar.edu.itba.pod.client;

public enum FileTypes {
    NEIGHBOURHOODS("barrios"),
    TREES("arboles");

    String fileType;

    FileTypes(String fileType) { this.fileType = fileType; }

    public String getFileType() { return fileType; }
}
