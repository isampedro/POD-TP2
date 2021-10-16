package ar.edu.itba.pod.api.Services;

import java.util.List;

public interface Query1 {
    public List<String> query(String city, String inCsvDirectory, String outCsvDirectory);
}
