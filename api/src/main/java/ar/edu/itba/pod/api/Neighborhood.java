package ar.edu.itba.pod.api;

public class Neighborhood {
    private final String name;
    private final long population;

    public Neighborhood(String name, long population) {
        this.name = name;
        this.population = population;
    }

    public String getName() {
        return name;
    }

    public long getPopulation() {
        return population;
    }
}
