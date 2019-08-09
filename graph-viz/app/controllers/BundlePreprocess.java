package controllers;

import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;

import java.util.*;

public class BundlePreprocess {

    ArrayList<Vector> dataNodes;
    ArrayList<EdgeVector> dataEdges;

    final int fromEdges = 0;
    final int toEdges = 1;
    final double fraction = 0.8;
    // indicate the number of threshold edge
    final int threshold = 5;

    public BundlePreprocess(ArrayList<Vector> dataNodes, ArrayList<EdgeVector> dataEdges) {
        this.dataNodes = dataNodes;
        this.dataEdges = dataEdges;
    }

    class Pair {
        int nodeId;
        int edgeId;

        public Pair(int nodeId, int edgeId) {
            this.nodeId = nodeId;
            this.edgeId = edgeId;
        }
    }

    public void process() {
        int dataNodeSize = this.dataNodes.size();
        HashMap<Integer, ArrayList<Pair>> adjacentMap = new HashMap<>();
        System.out.println("from before adjacency");
        insertAdjacency(dataNodeSize, adjacentMap, fromEdges);
        System.out.println("from end adjacency");
        // calculate the centroid of clump of targets shoot from every node
        bundleEdge(adjacentMap, dataNodeSize, fromEdges);
        System.out.println("from end bundle");
        insertAdjacency(dataNodeSize, adjacentMap, toEdges);
        System.out.println("to end adjacency");
        bundleEdge(adjacentMap, dataNodeSize, toEdges);
        System.out.println("to end bundle");

    }

    // type indicates the insertion type
    // 0: from edges
    // 1: to edges
    private void insertAdjacency(int dataNodeSize, HashMap<Integer, ArrayList<Pair>> adjacentMap, int type) {
        adjacentMap.clear();
        int size = dataEdges.size();
        ArrayList<Integer> removeEdgeId = new ArrayList<>();
        for (int i = size - 1; i >= 0; i--) {
            EdgeVector ev = dataEdges.get(i);
            int source = ev.sourceNodeInd;
            int target = ev.targetNodeInd;
            if (target < dataNodeSize) {
                if (type == fromEdges) {
                    adjacentMap.computeIfAbsent(source, k -> new ArrayList<>());
                    adjacentMap.get(source).add(new Pair(target, i));
                    removeEdgeAfterThreshold(removeEdgeId, adjacentMap, i, source);
                } else {
                    adjacentMap.computeIfAbsent(target, k -> new ArrayList<>());
                    adjacentMap.get(target).add(new Pair(source, i));
                    removeEdgeAfterThreshold(removeEdgeId, adjacentMap, i, target);
                }
            }
        }
        int removeEdgeIdSize = removeEdgeId.size();
        removeEdgeId.sort(Comparator.comparingInt(o -> o));
        for(int iter = removeEdgeIdSize - 1; iter >= 0; iter--) {
            this.dataEdges.remove(removeEdgeId.get(iter).intValue());
        }

    }

    private void removeEdgeAfterThreshold(ArrayList<Integer> removeEdgeId, HashMap<Integer, ArrayList<Pair>> adjacentMap, int i, int source) {
        if (adjacentMap.get(source).size() == threshold) {
            for (Pair tarPair : adjacentMap.get(source)) {
                int edgeId = tarPair.edgeId;
                removeEdgeId.add(edgeId);
            }
        } else if (adjacentMap.get(source).size() > threshold) {
            removeEdgeId.add(i);
        }
    }

    // type indicates the insertion type
    // 0: from edges
    // 1: to edges
    private void bundleEdge(HashMap<Integer, ArrayList<Pair>> adjacentMap, int dataNodeSize, int type) {
        for (int i = 0; i < dataNodeSize; i++) {
            if (adjacentMap.get(i) == null || adjacentMap.get(i).size() < threshold) {
                continue;
            }
            double originalX = this.dataNodes.get(i).x;
            double originalY = this.dataNodes.get(i).y;
            double centroidX = 0.0;
            double centroidY = 0.0;
            System.out.println("cent X: " + centroidX + ",cent Y: " + centroidY);
            int cnt = adjacentMap.get(i).size();
            int centroidInd = this.dataNodes.size();
            for (Pair tarPair : adjacentMap.get(i)) {
                int tarInd = tarPair.nodeId;
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
            centroidX = fraction * (centroidX - originalX) + originalX;
            centroidY = fraction * (centroidY - originalY) + originalY;
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
