package ar.edu.itba.pod.api;

public class Tree {
    private final String name;
    private final Neighborhood neighborhood;
    private final String street;

    public Tree(String name, Neighborhood neighborhood, String street) {
        this.name = name;
        this.neighborhood = neighborhood;
        this.street = street;
    }

    public String getName() {
        return name;
    }

    public Neighborhood getNeighborhood() {
        return neighborhood;
    }

    public String getStreet() {
        return street;
    }
}
