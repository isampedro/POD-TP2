package ar.edu.itba.pod.client;

import ar.edu.itba.pod.api.Neighborhood;
import ar.edu.itba.pod.api.Tree;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class CsvManager {

    private static final int COMUNA = 2;
    private static final int NOMBRE = 6;
    private static final int CALLE = 4;
    private static final int BARRIO = 0;
    private static final int POBLACION = 1;
    private static final String SEPARATOR = ";";

    public static void writeToCSV(String outPath, List<String> results, String headers) {
        results.add(0, headers);
        int i = 0;
        StringBuffer resultChunck = new StringBuffer();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(outPath + "/query1.csv"))) {
            for (String result : results) {
                System.out.println(result);
                resultChunck.append(result).append('\n');
                i++;
                if (i % 50 == 0) {
                    i = 0;
                    bw.write(resultChunck.toString());
                    resultChunck = new StringBuffer();
                }
            }
            if (!resultChunck.toString().equals("")) {
                bw.write(resultChunck.toString());
            }
        } catch (IOException e) {
            System.out.println("la cagamos");
            System.out.println(e.getMessage());
            // logger.error("IOException {} ",e.getMessage());
        }
    }

    public static QueryData readCsvData(String csvInPath, String city) {
        List<Neighborhood> neighborhoods = readNeighborhoodCsv(csvInPath, city);
        List<Tree> trees = readTreeCsv(csvInPath, city, neighborhoods);
        return new QueryData(trees, neighborhoods);
    }

    private static List<Tree> readTreeCsv(String csvInPath, String city, List<Neighborhood> neighborhoods) {
        ArrayList<Tree> trees = new ArrayList<>();
        String[] lineArgs;
        String line;
        boolean title = true;
        System.out.println("path barrios: " + csvInPath);
        String fileName = csvInPath + "/" + FileTypes.TREES.getFileType() + city + ".csv";

        try {

            FileReader fileReader = new FileReader(fileName);

            BufferedReader bufferedReader = new BufferedReader(fileReader);

            while ((line = bufferedReader.readLine()) != null) {
                if (title) {
                    title = false;
                } else {
                    lineArgs = line.split(SEPARATOR);
                    final String neighborhoodName = lineArgs[COMUNA];
                    trees.add(new Tree(lineArgs[NOMBRE],
                            neighborhoods.stream().filter(n -> n.getName().equals(neighborhoodName)).findAny()
                                    .orElse(new Neighborhood(neighborhoodName, 0)),
                            lineArgs[CALLE]));
                }
            }

            bufferedReader.close();
        } catch (FileNotFoundException ex) {
            System.out.println("Unable to open file '" + fileName + ".");
        } catch (IOException ex) {
            System.out.println("Error reading file '" + fileName + ".");
        }
        return trees;
    }

    private static List<Neighborhood> readNeighborhoodCsv(String csvInPath, String city) {
        ArrayList<Neighborhood> neighborhoods = new ArrayList<>();
        String[] lineArgs;
        String line;
        boolean title = true;
        System.out.println("path barrios: " + csvInPath);
        String fileName = csvInPath + "/" + FileTypes.NEIGHBOURHOODS.getFileType() + city + ".csv";

        try {

            FileReader fileReader = new FileReader(fileName);

            BufferedReader bufferedReader = new BufferedReader(fileReader);

            while ((line = bufferedReader.readLine()) != null) {
                if (title) {
                    title = false;
                } else {
                    lineArgs = line.split(SEPARATOR);
                    neighborhoods.add(new Neighborhood(lineArgs[BARRIO], Long.parseLong(lineArgs[POBLACION])));
                }
            }

            bufferedReader.close();
        } catch (FileNotFoundException ex) {
            System.out.println("Unable to open file '" + fileName + ".");
        } catch (IOException ex) {
            System.out.println("Error reading file '" + fileName + ".");
        }
        return neighborhoods;
    }
}
