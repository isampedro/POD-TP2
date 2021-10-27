package ar.edu.itba.pod.api;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.Objects;

public class Tree implements DataSerializable {
    private String name;
    private Neighborhood neighborhood;
    private String street;

    public Tree(String name, Neighborhood neighborhood, String street) {
        this.name = name;
        this.neighborhood = neighborhood;
        this.street = street;
    }

    public Tree() {

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

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        neighborhood.writeData(out);
        out.writeUTF(street);
        out.writeUTF(name);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.neighborhood = new Neighborhood();
        neighborhood.setName(in.readUTF());
        neighborhood.setPopulation(in.readLong());
        this.street = in.readUTF();
        this.name = in.readUTF();
    }
}
