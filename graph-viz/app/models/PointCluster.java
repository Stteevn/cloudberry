package models;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Stack;

public class PointCluster {

    private int minZoom;
    private int maxZoom;
    private double radius = 80;
    private double extent = 512;
    private KdTree[] trees;

    public PointCluster(int minZoom, int maxZoom) {
        this.minZoom = minZoom;
        this.maxZoom = maxZoom;
        trees = new KdTree[maxZoom + 2];
        for (int z = minZoom; z <= maxZoom + 1; z++) {
            trees[z] = new KdTree();
        }
    }

    public void load(ArrayList<Cluster> points) {
        for (Cluster point : points) {
            insert(new Cluster(lngX(point.x()), latY(point.y()), null, 1));
        }
    }

    public void insert(Cluster point) {
        for (int z = maxZoom; z >= minZoom; z--) {
            ArrayList<Cluster> neighbors = trees[z + 1].rangeRadius(point.x(), point.y(), getZoomRadius(z));
            if (neighbors.isEmpty()) {
                Cluster c = new Cluster(point.x(), point.y());
                c.setZoom(z);
                point.parent = c;
                trees[z + 1].insert(c);
                point = c;
            } else {
                Cluster neighbor = null;
                point.setZoom(z + 1);
                for (int i = 0; i < neighbors.size(); i++) {
                    neighbor = neighbors.get(i);
                    if (neighbor.x() != point.x() || neighbor.y() != point.y()) break;
                }
                point.parent = neighbor;
                while (neighbor != null) {
                    double wx = neighbor.x() * neighbor.getNumPoints() + point.x();
                    double wy = neighbor.y() * neighbor.getNumPoints() + point.y();
                    neighbor.setNumPoints(neighbor.getNumPoints() + 1);
                    neighbor.setX(wx / neighbor.getNumPoints());
                    neighbor.setY(wy / neighbor.getNumPoints());
                    neighbor = neighbor.parent;
                }
                break;
            }
        }
    }

    public double getZoomRadius(int zoom) {
        return radius / (extent * Math.pow(2, zoom));
    }

    public ArrayList<Cluster> cluster(ArrayList<Cluster> clusters, int zoom) {
        ArrayList<Cluster> results = new ArrayList<>();
        double r = getZoomRadius(zoom);
        for (int i = 0; i < clusters.size(); i++) {
            Cluster p = clusters.get(i);
            if (p.getZoom() <= zoom) continue;
            p.setZoom(zoom);
            KdTree kdTree = trees[zoom + 1];
            ArrayList<Cluster> neighbors = kdTree.rangeRadius(p.x(), p.y(), r);
            int numPoints = p.getNumPoints() > 0 ? p.getNumPoints() : 1;
            double wx = p.x() * numPoints;
            double wy = p.y() * numPoints;

            Cluster c = new Cluster(p.x(), p.y());
            p.parent = c;
            for (int j = 0; j < neighbors.size(); j++) {
                Cluster neighbor = neighbors.get(j);
                if (neighbor.getZoom() <= zoom) continue;
                neighbor.setZoom(zoom);
                int numPoints2 = neighbor.getNumPoints() > 0 ? neighbor.getNumPoints() : 1;
                wx += neighbor.x() * numPoints2;
                wy += neighbor.y() * numPoints2;
                numPoints += numPoints2;
                neighbor.parent = c;
            }

            if (numPoints == 1) {
                results.add(c);
            } else {
                c.setX(wx / numPoints);
                c.setY(wy / numPoints);
                c.setNumPoints(numPoints);
                results.add(c);
            }
        }
        System.out.printf("finish clustering %d points for %d zoom\n", results.size(), zoom);
        return results;
    }

    public ArrayList<Cluster> getClusters(double[] bbox, int zoom) {
        double minLongitude = ((bbox[0] + 180) % 360 + 360) % 360 - 180;
        double minLatitude = Math.max(-90, Math.min(90, bbox[1]));
        double maxLongitude = bbox[2] == 180 ? 180 : ((bbox[2] + 180) % 360 + 360) % 360 - 180;
        double maxLatitude = Math.max(-90, Math.min(90, bbox[3]));
        if (bbox[2] - bbox[0] >= 360) {
            minLongitude = -180;
            maxLongitude = 180;
        } else if (minLongitude > maxLongitude) {
            ArrayList<Cluster> results = getClusters(new double[]{minLongitude, minLatitude, 180, maxLatitude}, zoom);
            results.addAll(getClusters(new double[]{-180, minLatitude, maxLongitude, maxLatitude}, zoom));
            return results;
        }
        KdTree kdTree = trees[limitZoom(zoom + 1)];
        ArrayList<Cluster> neighbors = kdTree.range(new RectHV(lngX(minLongitude), latY(maxLatitude), lngX(maxLongitude), latY(minLatitude)));
        ArrayList<Cluster> clusters = new ArrayList<>();
        for (int i = 0; i < neighbors.size(); i++) {
            Cluster neighbor = neighbors.get(i);
            if (neighbor.getNumPoints() > 0) {
                clusters.add(neighbor);
            } else {
                clusters.add(neighbor);
            }
        }
        return clusters;
    }

    private int limitZoom(int z) {
        return Math.max(minZoom, Math.min(z, maxZoom + 1));
    }


    // longitude/latitude to spherical mercator in [0..1] range
    public static double lngX(double lng) {
        return lng / 360 + 0.5;
    }

    public static double latY(double lat) {
        double sin = Math.sin(lat * Math.PI / 180);
        double y = (0.5 - 0.25 * Math.log((1 + sin) / (1 - sin)) / Math.PI);
        return y < 0 ? 0 : y > 1 ? 1 : y;
    }

    // spherical mercator to longitude/latitude
    public static double xLng(double x) {
        return (x - 0.5) * 360;
    }

    public static double yLat(double y) {
        double y2 = (180 - y * 360) * Math.PI / 180;
        return 360 * Math.atan(Math.exp(y2)) / Math.PI - 90;
    }
}
