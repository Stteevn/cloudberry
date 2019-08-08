package controllers;

import java.util.*;

public class BundlePreprocess {

    ArrayList<Vector> dataNodes;
    ArrayList<EdgeVector> dataEdges;

    final int fromEdges = 0;
    final int toEdges = 1;

    public BundlePreprocess(ArrayList<Vector> dataNodes, ArrayList<EdgeVector> dataEdges) {
        this.dataNodes = dataNodes;
        this.dataEdges = dataEdges;
    }

    public void process() {
        int dataNodeSize = this.dataNodes.size();
        HashMap<Integer, ArrayList<Integer>> adjacentMap = new HashMap<>();
        insertAdjacency(dataNodeSize, adjacentMap, fromEdges);
        this.dataEdges.clear();
        // calculate the centroid of clump of targets shoot from every node
        bundleEdge(adjacentMap, dataNodeSize, fromEdges);
        insertAdjacency(dataNodeSize, adjacentMap, toEdges);
        bundleEdge(adjacentMap, dataNodeSize, toEdges);

    }

    // type indicates the insertion type
    // 0: from edges
    // 1: to edges
    private void insertAdjacency(int dataNodeSize, HashMap<Integer, ArrayList<Integer>> adjacentMap, int type) {
        adjacentMap.clear();
        int size = dataEdges.size();
        for (int i = size - 1; i >= 0; i--) {
            EdgeVector ev = dataEdges.get(i);
            int source = ev.sourceNodeInd;
            int target = ev.targetNodeInd;
            if (target < dataNodeSize) {
                if (type == fromEdges) {
                    adjacentMap.computeIfAbsent(source, k -> new ArrayList<>());
                    adjacentMap.get(source).add(target);
                } else {
                    adjacentMap.computeIfAbsent(target, k -> new ArrayList<>());
                    adjacentMap.get(target).add(source);
                    this.dataEdges.remove(i);
                }
            }
        }
    }

    // type indicates the insertion type
    // 0: from edges
    // 1: to edges
    private void bundleEdge(HashMap<Integer, ArrayList<Integer>> adjacentMap, int dataNodeSize, int type) {
        for (int i = 0; i < dataNodeSize; i++) {
            if (adjacentMap.get(i) == null || adjacentMap.get(i).size() == 0) {
                continue;
            }
            double centroidX = this.dataNodes.get(i).x;
            double centroidY = this.dataNodes.get(i).y;
            System.out.println("cent X: " + centroidX + ",cent Y: " + centroidY);
            int cnt = adjacentMap.get(i).size() + 1;
            int centroidInd = this.dataNodes.size();
            for (Integer tarInd : adjacentMap.get(i)) {
                Vector target = dataNodes.get(tarInd);
                centroidX += target.x;
                centroidY += target.y;
//                System.out.println("from: " + centroidInd + ",to: " + tarInd);
                if (type == fromEdges) {
                    this.dataEdges.add(new EdgeVector(centroidInd, tarInd));
                } else {
                    this.dataEdges.add(new EdgeVector(tarInd, centroidInd));
                }
            }
            centroidX = centroidX / (cnt * 1.0);
            centroidY = centroidY / (cnt * 1.0);
//            System.out.println("cent X: " + centroidX + ",cent Y: " + centroidY);
            Vector centroid = new Vector(centroidX, centroidY);
            this.dataNodes.add(centroid);
//            System.out.println("from: " + i + ",to: " + centroidInd);
            if (type == fromEdges) {
                this.dataEdges.add(new EdgeVector(i, centroidInd));
            } else {
                this.dataEdges.add(new EdgeVector(centroidInd, i));
            }
        }
    }

//    public static void main(String[] args) {
//        Vector v1 = new Vector(1.0, 2.0);
//        Vector v2 = new Vector(3.0, 4.0);
//        Vector v3 = new Vector(5.0, 6.0);
//        Vector v4 = new Vector(7.0, 8.0);
//        Vector v5 = new Vector(9.0, 10.0);
//        Vector v6 = new Vector(11.0, 12.0);
//        ArrayList<Vector> dataNodes = new ArrayList<>();
//        dataNodes.add(v1);
//        dataNodes.add(v2);
//        dataNodes.add(v3);
//        dataNodes.add(v4);
//        dataNodes.add(v5);
//        dataNodes.add(v6);
//        ArrayList<EdgeVector> dataEdges = new ArrayList<>();
//        dataEdges.add(new EdgeVector(0, 1));
//        dataEdges.add(new EdgeVector(0, 2));
//        dataEdges.add(new EdgeVector(1, 2));
//        dataEdges.add(new EdgeVector(1, 3));
//        dataEdges.add(new EdgeVector(3, 4));
//        dataEdges.add(new EdgeVector(3, 5));
//        BundlePreprocess bp = new BundlePreprocess(dataNodes, dataEdges);
//        bp.process();
//        for(Vector v: bp.dataNodes){
//            System.out.println("x: " + v.x + ", y: " + v.y);
//        }
//        for(EdgeVector ev: bp.dataEdges){
//            System.out.println("source: " + ev.sourceNodeInd + ", target: " + ev.targetNodeInd);
//        }
//    }
}
