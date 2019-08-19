package controllers;

import java.util.Objects;

public class Edge {
    private double fromLatitude;
    private double fromLongitude;
    private double toLatitude;
    private double toLongitude;

    public Edge(double fromLatitude, double fromLongitude, double toLatitude, double toLongitude, int weight) {
        this.fromLatitude = fromLatitude;
        this.fromLongitude = fromLongitude;
        this.toLatitude = toLatitude;
        this.toLongitude = toLongitude;
        this.weight = weight;
    }

    public Edge updateWeight(int num) {
        Edge newEdge = this;
        newEdge.weight += num;
        return newEdge;
    }

    public int getWeight() {
        return weight;
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }

    private int weight;

    public static double get_epsilon() {
        return _epsilon;
    }

    public static void set_epsilon(double _epsilon) {
        Edge._epsilon = _epsilon;
    }

    private static double _epsilon = 10;

    public Edge() {
    }

    public Edge(double fromLatitude, double fromLongitude, double toLatitude, double toLongitude) {
        this.fromLatitude = fromLatitude;
        this.fromLongitude = fromLongitude;
        this.toLatitude = toLatitude;
        this.toLongitude = toLongitude;
    }

    public double getFromLatitude() {
        return this.fromLatitude;
    }

    public void setFromLatitude(double fromLatitude) {
        this.fromLatitude = fromLatitude;
    }

    public double getFromLongitude() {
        return this.fromLongitude;
    }

    public void setFromLongitude(double fromLongitude) {
        this.fromLongitude = fromLongitude;
    }

    public double getToLatitude() {
        return this.toLatitude;
    }

    public void setToLatitude(double toLatitude) {
        this.toLatitude = toLatitude;
    }

    public double getToLongitude() {
        return this.toLongitude;
    }

    public void setToLongitude(double toLongitude) {
        this.toLongitude = toLongitude;
    }

    public Edge fromLatitude(double fromLatitude) {
        this.fromLatitude = fromLatitude;
        return this;
    }

    public Edge fromLongtitude(double fromLongtitude) {
        this.fromLongitude = fromLongtitude;
        return this;
    }

    public Edge toLatitude(double toLatitude) {
        this.toLatitude = toLatitude;
        return this;
    }

    public Edge toLongtitude(double toLongtitude) {
        this.toLongitude = toLongtitude;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof Edge)) {
            return false;
        }
        Edge edge = (Edge) o;
        return fromLatitude == edge.fromLatitude && fromLongitude == edge.fromLongitude && toLatitude == edge.toLatitude && toLongitude == edge.toLongitude;
    }

    public boolean cross(Edge e2) {
        return ((toLongitude - fromLongitude) * (e2.fromLatitude - fromLatitude)
                - (e2.fromLongitude - fromLongitude) * (toLatitude - fromLatitude))
                * ((toLongitude - fromLongitude) * (e2.toLatitude - fromLatitude)
                - (e2.toLongitude - fromLongitude) * (toLatitude - fromLatitude)) < 0
                && ((e2.toLongitude - e2.fromLongitude) * (fromLatitude - e2.fromLatitude)
                - (fromLongitude - e2.fromLongitude) * (e2.toLatitude - e2.fromLatitude))
                * ((e2.toLongitude - e2.fromLongitude) * (toLatitude - e2.fromLatitude)
                - (toLongitude - e2.fromLongitude) * (e2.toLatitude - e2.fromLatitude)) < 0;
    }

    private int getBlockNum(double longitude, double latitude) {
        return (int) ((longitude + 180) / _epsilon) + (int) ((latitude + 90) / _epsilon) * (int) (360 / _epsilon);
    }

    public int getBlock() {
        return getBlockNum(fromLongitude, fromLatitude) * (int) (360 / _epsilon) * (int) (180 / _epsilon) + getBlockNum(toLongitude, toLatitude);
    }

    @Override
    public int hashCode() {
        if(fromLatitude < toLatitude) {
            return Objects.hash(fromLatitude, fromLongitude, toLatitude, toLongitude);
        }else{
            return Objects.hash(toLatitude, toLongitude, fromLatitude, fromLongitude);
        }
    }

    @Override
    public String toString() {
        return "{" +
                "\"source\":" + getFromLongitude() + "," + getFromLatitude() +
                ",\"target\":" + getToLongitude() + "," + getToLatitude() +
                "}";
    }

    public double getDegree() {
        return Math.toDegrees(Math.atan((getToLatitude() - getFromLatitude()) / (getToLongitude() - getFromLongitude())));
    }
}