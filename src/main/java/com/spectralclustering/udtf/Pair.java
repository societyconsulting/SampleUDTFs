package com.spectralclustering.udtf;


/**
 * Generic Pair class. Holds any two elements.
 *
 * @param <A>
 * @param <B>
 */
public class Pair<A, B> {
    private A first;
    private B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    /**
     * The hash code must also be symmetric since equals is symmetric.
     *
     * @return a symmetric hashcode for fields first and second.
     */
    public int hashCode() {
        int hashFirst = first != null ? first.hashCode() : 0;
        int hashSecond = second != null ? second.hashCode() : 0;

        // hash code has to be symmetric since equals is symmetric, e.g.
        // hash(A,B) == hash(B,A)
        return (int)(hashFirst ^ (hashSecond >>> 32));
    }

    /**
     * Returns true if (this.first == other.first and this.second ==
     * other.second) or (this.first == other.second and this.second ==
     * other.first).
     *
     * @param other The other pair.
     * @return
     */
    public boolean equals(Object other) {
        if (other instanceof Pair) {
            Pair otherPair = (Pair) other;
            boolean t1 = ((this.first == otherPair.first ||
                    (this.first != null && otherPair.first != null &&
                            this.first.equals(otherPair.first))) &&
                    (this.second == otherPair.second ||
                            (this.second != null && otherPair.second != null &&
                                    this.second.equals(otherPair.second))));

            boolean t2 = ((this.first == otherPair.second ||
                    (this.first != null && otherPair.second != null &&
                            this.first.equals(otherPair.second))) &&
                    (this.second == otherPair.first ||
                            (this.second != null && otherPair.first != null &&
                                    this.second.equals(otherPair.first))));

            return t1 || t2;
        }

        return false;
    }

    public String toString() {
        return "(" + first + ", " + second + ")";
    }

    /* Getters */
    public A getFirst() { return first; }
    public B getSecond() { return second; }

    /* Setters */
    public void setFirst(A first) { this.first = first; }
    public void setSecond(B second) { this.second = second; }
}
