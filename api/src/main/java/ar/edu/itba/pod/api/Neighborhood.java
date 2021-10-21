package ar.edu.itba.pod.api;

import java.util.Objects;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Neighborhood that = (Neighborhood) o;
        return population == that.population && name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, population);
    }
}
