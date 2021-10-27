package ar.edu.itba.pod.client;

import ar.edu.itba.pod.api.Neighborhood;
import ar.edu.itba.pod.api.Tree;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class CsvManager {
    public static void writeToCSV(String outPath, List<String> results, String headers){
        System.out.println("antes de escrirbir en el write");
        results.add(0, headers);
        int i = 0;
        StringBuffer resultChunck = new StringBuffer();
        try(BufferedWriter bw = new BufferedWriter(new FileWriter(outPath + "/query1.csv"))) {
            System.out.println("adentro del try");
            System.out.println("resultados: " + results.size());
            for (String result : results) {
                System.out.println(result);
                resultChunck.append(result).append('\n');
                System.out.println("append");
                i++;
                if(i % 50 == 0) {
                    i = 0;
                    System.out.println("write");
                    bw.write(resultChunck.toString());
                    resultChunck = new StringBuffer();
                }
            }
            System.out.println("pase el for?");
            if(!resultChunck.toString().equals("")) {
                System.out.println("estoy escribiendo?");
                bw.write(resultChunck.toString());
            }
        } catch (IOException e) {
            System.out.println("la cagamos");
            System.out.println(e.getMessage());
            //logger.error("IOException {} ",e.getMessage());
        }
        System.out.println("me quede colgado");
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

            BufferedReader bufferedReader =
                    new BufferedReader(fileReader);

            while((line = bufferedReader.readLine()) != null) {
                if(title) {
                    title = false;
                } else {
                    lineArgs = line.split(",");
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
        System.out.println("path barrios: " + csvInPath);
        String fileName = csvInPath + "/" + FileTypes.NEIGHBOURHOODS.getFileType() + city + ".csv";

        try {

            FileReader fileReader = new FileReader(fileName);

            BufferedReader bufferedReader =
                    new BufferedReader(fileReader);

            while((line = bufferedReader.readLine()) != null) {
                if(title) {
                    title = false;
                } else {
                    lineArgs = line.split(",");
                    neighborhoods.add(new Neighborhood(lineArgs[0], Long.parseLong(lineArgs[1])));
                }
            }

            bufferedReader.close();
        } catch(FileNotFoundException ex) {
            System.out.println("Unable to open file '" + fileName + ".");
        } catch(IOException ex) {
            System.out.println("Error reading file '" + fileName + ".");
        }
        System.out.println("barrios en levantada de archivo");
        neighborhoods.forEach(neighborhood -> System.out.println(neighborhood.getName() + " " + neighborhood.getPopulation()));
        return neighborhoods;
    }
}
