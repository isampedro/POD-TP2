package ar.edu.itba.pod.api;

import java.io.Serializable;
import java.util.Objects;


public class Pair<A extends Serializable & Comparable<A>, B extends Serializable> implements Comparable<A>, Serializable {

    public A fst;
    public B snd;

    public Pair(A fst, B snd) {
        this.fst = fst;
        this.snd = snd;
    }

    public Pair() {

    }

    public String toString() {
        return fst + ";" + snd;
    }

    public boolean equals(Object other) {
        return
                other instanceof Pair<?,?> &&
                        Objects.equals(fst, ((Pair<?,?>)other).fst) &&
                        Objects.equals(snd, ((Pair<?,?>)other).snd);
    }

    public int hashCode() {
        if (fst == null) return (snd == null) ? 0 : snd.hashCode() + 1;
        else if (snd == null) return fst.hashCode() + 2;
        else return fst.hashCode() * 17 + snd.hashCode();
    }

    public static <A extends Serializable & Comparable<A>, B extends Serializable> Pair<A,B> of(A a, B b) {
        return new Pair<>(a,b);
    }

    @Override
    public int compareTo(A a) {
        return this.fst.compareTo(a);
    }
}