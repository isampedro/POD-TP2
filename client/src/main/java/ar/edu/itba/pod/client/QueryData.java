package ar.edu.itba.pod.client;

import ar.edu.itba.pod.api.Neighborhood;
import ar.edu.itba.pod.api.Tree;

import java.util.List;

public class QueryData {

    private final List<Tree> trees;
    private final List<Neighborhood> neighborhoods;

    public QueryData(List<Tree> trees, List<Neighborhood> neighborhoods) {
        this.trees = trees;
        this.neighborhoods = neighborhoods;
    }

    public List<Tree> getTrees() {
        return trees;
    }

    public List<Neighborhood> getNeighborhoods() {
        return neighborhoods;
    }
}
