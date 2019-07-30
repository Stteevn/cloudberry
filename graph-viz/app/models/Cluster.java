package models;

import java.util.Comparator;


/**
 * The <tt>Point</tt> class is an immutable data type to encapsulate a
 * two-dimensional point with real-value coordinates.
 * <p>
 * Note: in order to deal with the difference behavior of double and
 * Double with respect to -0.0 and +0.0, the Cluster constructor converts
 * any coordinates that are -0.0 to +0.0.
 * <p>
 * For additional documentation, see <a href="/algs4/12oop">Section 1.2</a> of
 * <i>Algorithms, 4th Edition</i> by Robert Sedgewick and Kevin Wayne.
 *
 * @author Robert Sedgewick
 * @author Kevin Wayne
 */
public class Cluster implements Comparable<Cluster> {

    /**
     * Compares two points by x-coordinate.
     */
    public static final Comparator<Cluster> X_ORDER = new XOrder();

    /**
     * Compares two points by y-coordinate.
     */
    public static final Comparator<Cluster> Y_ORDER = new YOrder();

    private double x;    // x coordinate
    private double y;    // y coordinate
    private int numPoints = 0;
    private int zoom = 0;
    public Cluster parent;

    public int getZoom() {
        return zoom;
    }

    public void setZoom(int zoom) {
        this.zoom = zoom;
    }


    public int getNumPoints() {
        return numPoints;
    }

    public void setNumPoints(int numPoints) {
        this.numPoints = numPoints;
    }

    /**
     * Initializes a new point (x, y).
     *
     * @param x the x-coordinate
     * @param y the y-coordinate
     * @throws IllegalArgumentException if either <tt>x</tt> or <tt>y</tt>
     *                                  is <tt>Double.NaN</tt>, <tt>Double.POSITIVE_INFINITY</tt> or
     *                                  <tt>Double.NEGATIVE_INFINITY</tt>
     */
    public Cluster(double x, double y) {
        if (Double.isInfinite(x) || Double.isInfinite(y))
            throw new IllegalArgumentException("Coordinates must be finite");
        if (Double.isNaN(x) || Double.isNaN(y))
            throw new IllegalArgumentException("Coordinates cannot be NaN");
        if (x == 0.0) x = 0.0;  // convert -0.0 to +0.0
        if (y == 0.0) y = 0.0;  // convert -0.0 to +0.0
        this.x = x;
        this.y = y;
        this.numPoints = 1;
        this.zoom = Integer.MAX_VALUE;
    }

    public Cluster(double x, double y, Cluster parent, int numPoints) {
        this.x = x;
        this.y = y;
        this.parent = parent;
        this.numPoints = numPoints;
        this.zoom = Integer.MAX_VALUE;
    }

    /**
     * Returns the x-coordinate.
     *
     * @return the x-coordinate
     */
    public double x() {
        return x;
    }

    /**
     * Returns the y-coordinate.
     *
     * @return the y-coordinate
     */
    public double y() {
        return y;
    }

    public void setX(double x) {
        this.x = x;
    }

    public void setY(double y) {
        this.y = y;
    }

    /**
     * Returns the polar radius of this point.
     *
     * @return the polar radius of this point in polar coordiantes: sqrt(x*x + y*y)
     */
    public double r() {
        return Math.sqrt(x * x + y * y);
    }

    /**
     * Returns the angle of this point in polar coordinates.
     *
     * @return the angle (in radians) of this point in polar coordiantes (between -pi/2 and pi/2)
     */
    public double theta() {
        return Math.atan2(y, x);
    }

    /**
     * Returns the angle between this point and that point.
     *
     * @return the angle in radians (between -pi and pi) between this point and that point (0 if equal)
     */
    private double angleTo(Cluster that) {
        double dx = that.x - this.x;
        double dy = that.y - this.y;
        return Math.atan2(dy, dx);
    }

    /**
     * Is a->b->c a counterclockwise turn?
     *
     * @param a first point
     * @param b second point
     * @param c third point
     * @return { -1, 0, +1 } if a->b->c is a { clockwise, collinear; counterclocwise } turn.
     */
    public static int ccw(Cluster a, Cluster b, Cluster c) {
        double area2 = (b.x - a.x) * (c.y - a.y) - (b.y - a.y) * (c.x - a.x);
        if (area2 < 0) return -1;
        else if (area2 > 0) return +1;
        else return 0;
    }

    /**
     * Returns twice the signed area of the triangle a-b-c.
     *
     * @param a first point
     * @param b second point
     * @param c third point
     * @return twice the signed area of the triangle a-b-c
     */
    public static double area2(Cluster a, Cluster b, Cluster c) {
        return (b.x - a.x) * (c.y - a.y) - (b.y - a.y) * (c.x - a.x);
    }

    /**
     * Returns the Euclidean distance between this point and that point.
     *
     * @param that the other point
     * @return the Euclidean distance between this point and that point
     */
    public double distanceTo(Cluster that) {
        double dx = this.x - that.x;
        double dy = this.y - that.y;
        return Math.sqrt(dx * dx + dy * dy);
    }

    /**
     * Returns the square of the Euclidean distance between this point and that point.
     *
     * @param that the other point
     * @return the square of the Euclidean distance between this point and that point
     */
    public double distanceSquaredTo(Cluster that) {
        double dx = this.x - that.x;
        double dy = this.y - that.y;
        return dx * dx + dy * dy;
    }

    /**
     * Compares this point to that point by y-coordinate, breaking ties by x-coordinate.
     *
     * @param that the other point
     * @return { a negative integer, zero, a positive integer } if this point is
     * { less than, equal to, greater than } that point
     */
    public int compareTo(Cluster that) {
        if (this.y < that.y) return -1;
        if (this.y > that.y) return +1;
        if (this.x < that.x) return -1;
        if (this.x > that.x) return +1;
        return 0;
    }

    // compare points according to their x-coordinate
    private static class XOrder implements Comparator<Cluster> {
        public int compare(Cluster p, Cluster q) {
            if (p.x < q.x) return -1;
            if (p.x > q.x) return +1;
            return 0;
        }
    }

    // compare points according to their y-coordinate
    private static class YOrder implements Comparator<Cluster> {
        public int compare(Cluster p, Cluster q) {
            if (p.y < q.y) return -1;
            if (p.y > q.y) return +1;
            return 0;
        }
    }

    /**
     * Does this point equal y?
     *
     * @param other the other point
     * @return true if this point equals the other point; false otherwise
     */
    public boolean equals(Object other) {
        if (other == this) return true;
        if (other == null) return false;
        if (other.getClass() != this.getClass()) return false;
        Cluster that = (Cluster) other;
        return this.x == that.x && this.y == that.y;
    }

    /**
     * Return a string representation of this point.
     *
     * @return a string representation of this point in the format (x, y)
     */
    public String toString() {
        return String.format("(%.2f, %.2f), zoom: %d, numPoints: %d", PointCluster.xLng(x), PointCluster.yLat(y), zoom, numPoints);
    }

    /**
     * Returns an integer hash code for this point.
     *
     * @return an integer hash code for this point
     */
    public int hashCode() {
        int hashX = ((Double) x).hashCode();
        int hashY = ((Double) y).hashCode();
        return 31 * hashX + hashY;
    }
}
