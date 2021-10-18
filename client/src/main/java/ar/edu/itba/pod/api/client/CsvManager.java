package ar.edu.itba.pod.api.client;

import ar.edu.itba.pod.api.Neighborhood;
import ar.edu.itba.pod.api.Tree;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class CsvManager {
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
        String fileName = csvInPath + "/" + FileTypes.TREES.getFileType() + city;

        try {

            FileReader fileReader = new FileReader(fileName);

            BufferedReader bufferedReader =
                    new BufferedReader(fileReader);

            while((line = bufferedReader.readLine()) != null) {
                if(title) {
                    title = false;
                } else {
                    lineArgs = line.split(";");
                    final String neighborhoodName = lineArgs[2];
                    trees.add(new Tree(lineArgs[1],
                            neighborhoods
                                    .stream()
                                    .filter(n -> n.getName().equals(neighborhoodName))
                                    .findAny()
                                    .orElse(new Neighborhood(neighborhoodName, 0)), lineArgs[0]));
                }
            }

            bufferedReader.close();
        } catch(FileNotFoundException ex) {
            System.out.println("Unable to open file '" + fileName + ".");
        } catch(IOException ex) {
            System.out.println("Error reading file '" + fileName + ".");
        }
        return trees;
    }

    private static List<Neighborhood> readNeighborhoodCsv(String csvInPath, String city) {
        ArrayList<Neighborhood> neighborhoods = new ArrayList<>();
        String[] lineArgs;
        String line;
        boolean title = true;
        String fileName = csvInPath + "/" + FileTypes.NEIGHBOURHOODS.getFileType() + city;

        try {

            FileReader fileReader = new FileReader(fileName);

            BufferedReader bufferedReader =
                    new BufferedReader(fileReader);

            while((line = bufferedReader.readLine()) != null) {
                if(title) {
                    title = false;
                } else {
                    lineArgs = line.split(";");
                    neighborhoods.add(new Neighborhood(lineArgs[0], Long.parseLong(lineArgs[1])));
                }
            }

            bufferedReader.close();
        } catch(FileNotFoundException ex) {
            System.out.println("Unable to open file '" + fileName + ".");
        } catch(IOException ex) {
            System.out.println("Error reading file '" + fileName + ".");
        }
        return neighborhoods;
    }
}
