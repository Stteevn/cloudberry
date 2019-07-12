package controllers;

import java.util.Objects;

public class Edge {
    private double fromLatitude;
    private double fromLongitude;
    private double toLatitude;
    private double toLongitude;

    public static double get_epslion() {
        return _epslion;
    }

    public static void set_epslion(double _epslion) {
        Edge._epslion = _epslion;
    }

    private static double _epslion = 10;

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
        if (_epslion == 0) {
            return fromLatitude == edge.fromLatitude && fromLongitude == edge.fromLongitude && toLatitude == edge.toLatitude && toLongitude == edge.toLongitude;
        } else {
            return getBlockNum(fromLongitude, fromLatitude) == getBlockNum(edge.fromLongitude, edge.fromLatitude)
                    && getBlockNum(toLongitude, toLatitude) == getBlockNum(edge.toLongitude, edge.toLatitude);
        }
    }

    private int getBlockNum(double longitude, double latitude) {
        return (int) ((longitude + 180) / _epslion) + (int) ((latitude + 90) / _epslion) * (int) (360 / _epslion);
    }

    @Override
    public int hashCode() {
        if (_epslion == 0) {
            return Objects.hash(fromLatitude, fromLongitude, toLatitude, toLongitude);
        } else {
            return Objects.hash(getBlockNum(fromLongitude, fromLatitude), getBlockNum(toLongitude, toLatitude));
        }
    }

    @Override
    public String toString() {
        return "{" +
                "\"source\":" + getFromLongitude() + "," + getFromLatitude() +
                ",\"target\":" + getToLongitude() + "," + getToLatitude() +
                "}";
    }

}