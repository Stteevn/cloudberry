package algorithms;

import models.Cluster;
import models.Edge;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class TreeCut {

    public void treeCut(PointCluster pointCluster, double lowerLongitude, double upperLongitude, double lowerLatitude, double upperLatitude, int zoom, HashMap<Edge, Integer> edges, HashSet<Edge> externalEdgeSet, HashSet<Cluster> externalCluster, HashSet<Cluster> internalCluster) {
        HashMap<Cluster, Cluster> externalClusterMap = new HashMap<>();
        HashMap<Cluster, ArrayList<Cluster>> externalHierarchy = new HashMap<>();
        HashSet<Cluster> internalHierarchy = new HashSet<>();
        addClusterHierarchy(externalCluster, externalHierarchy, externalClusterMap);
        addClusterHierarchy(internalCluster, internalHierarchy);
        while (true) {
            ArrayList<Cluster> removeKeyList = new ArrayList<>();
            // Find clusters has common ancestor with internal clusters at this level
            for (Map.Entry<Cluster, ArrayList<Cluster>> entry : externalHierarchy.entrySet()) {
                Cluster key = entry.getKey();
                if (internalHierarchy.contains(key)) {
                    int level = entry.getKey().getZoom();
                    // use a level lower because of conflict
                    int diff = entry.getValue().get(0).getZoom() - level - 1;
                    Cluster cPar;
                    for (Cluster c : entry.getValue()) {
                        cPar = c;
                        for (int i = 0; i < diff; i++) {
                            cPar = cPar.parent;
                        }
                        externalClusterMap.put(c, cPar);
                    }
                    removeKeyList.add(key);
                }
            }
            // remove all clusters stop at this level
            for (Cluster c : removeKeyList) {
                externalHierarchy.remove(c);
            }
            if (externalHierarchy.size() == 0) {
                break;
            }
            // elevate all remaining clusters to a higher level
            externalHierarchy = elevateHierarchy(externalHierarchy, externalClusterMap);
            internalHierarchy = elevateHierarchy(internalHierarchy);
        }
        for (Edge edge : externalEdgeSet) {
            Cluster fromCluster = pointCluster.parentCluster(new Cluster(PointCluster.lngX(edge.getFromLongitude()), PointCluster.latY(edge.getFromLatitude())), zoom);
            Cluster toCluster = pointCluster.parentCluster(new Cluster(PointCluster.lngX(edge.getToLongitude()), PointCluster.latY(edge.getToLatitude())), zoom);
            double fromLongitude = PointCluster.xLng(fromCluster.x());
            double fromLatitude = PointCluster.yLat(fromCluster.y());
            double insideLat, insideLng, outsideLat, outsideLng;
            if (lowerLongitude <= fromLongitude && fromLongitude <= upperLongitude
                    && lowerLatitude <= fromLatitude && fromLatitude <= upperLatitude) {
                insideLng = fromLongitude;
                insideLat = fromLatitude;
                Cluster elevatedCluster = externalClusterMap.get(toCluster);
                outsideLng = PointCluster.xLng(elevatedCluster.x());
                outsideLat = PointCluster.yLat(elevatedCluster.y());
            } else {
                insideLng = PointCluster.xLng(toCluster.x());
                insideLat = PointCluster.yLat(toCluster.y());
                Cluster elevatedCluster = externalClusterMap.get(fromCluster);
                outsideLng = PointCluster.xLng(elevatedCluster.x());
                outsideLat = PointCluster.yLat(elevatedCluster.y());
            }
            Edge e = new Edge(insideLat, insideLng, outsideLat, outsideLng);
            if (edges.containsKey(e)) {
                edges.put(e, edges.get(e) + 1);
            } else {
                edges.put(e, 1);
            }
        }
    }

    private HashSet<Cluster> elevateHierarchy(HashSet<Cluster> internalHierarchy) {
        HashSet<Cluster> tempInternalHierarchy = new HashSet<>();
        for (Cluster c : internalHierarchy) {
            if (c.parent != null) {
                tempInternalHierarchy.add(c.parent);
            }
        }
        return tempInternalHierarchy;
    }

    private HashMap<Cluster, ArrayList<Cluster>> elevateHierarchy(HashMap<Cluster, ArrayList<Cluster>> externalHierarchy, HashMap<Cluster, Cluster> externalClusterMap) {
        HashMap<Cluster, ArrayList<Cluster>> tempExternalHierarchy = new HashMap<>();
        for (Map.Entry<Cluster, ArrayList<Cluster>> entry : externalHierarchy.entrySet()) {
            Cluster key = entry.getKey();
            if (key.parent != null) {
                if (!tempExternalHierarchy.containsKey(key.parent)) {
                    tempExternalHierarchy.put(key.parent, new ArrayList<>());
                }
                tempExternalHierarchy.get(key.parent).addAll(entry.getValue());
            }
            // has arrived highest level
            else {
                // use level 1 to make the elevation
                int diff = entry.getValue().get(0).getZoom() - 1;
                for (Cluster c : entry.getValue()) {
                    Cluster cPar = c;
                    for (int i = 0; i < diff; i++) {
                        cPar = cPar.parent;
                    }
                    externalClusterMap.put(c, cPar);
                }
            }
        }
        return tempExternalHierarchy;
    }

    private void addClusterHierarchy(HashSet<Cluster> externalCluster, HashSet<Cluster> internalHierarchy) {
        for (Cluster c : externalCluster) {
            if (c.parent != null) {
                internalHierarchy.add(c.parent);
            }
        }
    }

    private void addClusterHierarchy(HashSet<Cluster> externalCluster, HashMap<Cluster, ArrayList<Cluster>> externalHierarchy, HashMap<Cluster, Cluster> externalClusterMap) {
        for (Cluster c : externalCluster) {
            if (c.parent != null) {
                if (!externalHierarchy.containsKey(c.parent)) {
                    externalHierarchy.put(c.parent, new ArrayList<>());
                }
                externalHierarchy.get(c.parent).add(c);
            }
            // has arrived highest level
            else {
                // use level 1 to make the elevation
                externalClusterMap.put(c, c);
            }
        }

    }

    public void nonTreeCut(PointCluster pointCluster, int zoom, HashMap<Edge, Integer> edges, HashSet<Edge> externalEdgeSet) {
        for (Edge edge : externalEdgeSet) {
            Cluster fromCluster = pointCluster.parentCluster(new Cluster(PointCluster.lngX(edge.getFromLongitude()), PointCluster.latY(edge.getFromLatitude())), zoom);
            Cluster toCluster = pointCluster.parentCluster(new Cluster(PointCluster.lngX(edge.getToLongitude()), PointCluster.latY(edge.getToLatitude())), zoom);
            double fromLongitude = PointCluster.xLng(fromCluster.x());
            double fromLatitude = PointCluster.yLat(fromCluster.y());
            double toLongitude = PointCluster.xLng(toCluster.x());
            double toLatitude = PointCluster.yLat(toCluster.y());
            Edge e = new Edge(fromLatitude, fromLongitude, toLatitude, toLongitude);
            if (edges.containsKey(e)) {
                edges.put(e, edges.get(e) + 1);
            } else {
                edges.put(e, 1);
            }
        }
    }
}
