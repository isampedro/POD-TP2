package ar.edu.itba.pod.api;

import java.util.Objects;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Tree tree = (Tree) o;
        return name.equals(tree.name) && neighborhood.equals(tree.neighborhood) && street.equals(tree.street);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, neighborhood, street);
    }
}
