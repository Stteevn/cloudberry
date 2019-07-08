package controllers;

import java.lang.reflect.Array;
import java.util.ArrayList;

class Node {
    int id;
    double x;
    double y;
}

class Edge {
    int sourceNodeInd;
    int targetNodeInd;
}

class Vector {
    double x;
    double y;

    Vector(double x, double y) {
        this.x = x;
        this.y = y;
    }
}

class Path {
    ArrayList<Node> alv;

    Path() {
        alv = new ArrayList<Node>();
    }
}

public class ForceBundling {

    static ArrayList<Node> aln = new ArrayList<>();
    static ArrayList<Edge> ale = new ArrayList<>();
    static ArrayList<Path> subdivision = new ArrayList<>();
    static ArrayList<ArrayList<Edge>> compatibility = new ArrayList<>();
    final static double K = 0.1;
    final static double S_initial = 0.1;
    final static double P_initial = 1;
    final static double P_rate = 2;
    final static double C = 6;
    final static double I_initial = 90;
    final static double I_rate = 0.6666667;
    final static double compatibility_threshold = 0.6;
    final static double eps = 1e-6;

    double vectorDotProduct(Vector p, Vector q) {
        return p.x * q.x + p.y * q.y;
    }

    Vector edgeAsVector(Edge e) {
        return new Vector(aln.get(e.targetNodeInd).x - aln.get(e.sourceNodeInd).x, aln.get(e.targetNodeInd).y - aln.get(e.sourceNodeInd).y);
    }

    double edgeLength(Edge e) {
        if (Math.abs(aln.get(e.sourceNodeInd).x - aln.get(e.targetNodeInd).x) < eps &&
                Math.abs(aln.get(e.sourceNodeInd).y - aln.get(e.targetNodeInd).y) < eps) {
            return eps;
        }
        return Math.sqrt(Math.pow(aln.get(e.sourceNodeInd).x - aln.get(e.targetNodeInd).x, 2) +
                Math.pow(aln.get(e.sourceNodeInd).y - aln.get(e.targetNodeInd).y, 2));
    }

    // v1 for source, v2 for target
    double customEdgeLength(Vector v1, Vector v2) {
        return Math.sqrt(Math.pow(v1.x - v2.x, 2) + Math.pow(v1.y - v2.y, 2));
    }

    Vector edgeMidPoint(Edge e) {
        double midX = (aln.get(e.sourceNodeInd).x + aln.get(e.targetNodeInd).x) / 2.0;
        double midY = (aln.get(e.sourceNodeInd).y + aln.get(e.targetNodeInd).y) / 2.0;
        return new Vector(midX, midY);
    }

    double euclideanDistance(Node p, Node q) {
        return Math.sqrt(Math.pow(p.x - q.x, 2) + Math.pow(p.y - q.y, 2));
    }

    double computeDividedEdgeLength(int e_ind) {
        double length = 0;
        for (int i = 1; i < subdivision.get(e_ind).alv.size(); i++) {
            double segmentLength = euclideanDistance(subdivision.get(e_ind).alv.get(i), subdivision.get(e_ind).alv.get(i - 1));
            length += segmentLength;
        }
        return length;
    }

    // q1 for source and q2 for target
    Vector projectPointOnLine(Vector p, Vector q1, Vector q2) {
        double L = Math.sqrt(Math.pow(q2.x - q1.x, 2) + Math.pow(q2.y - q1.y, 2));
        double r = ((q1.y - p.y) * (q1.y - q2.y) - (q1.x - p.x) * (q2.x - q1.x)) / (Math.pow(L, 2));
        double x = q1.x + r * (q2.x - q1.x);
        double y = q1.y + r * (q2.y - q1.y);
        return new Vector(x, y);
    }

    void initializeEdgeSubdivisions() {
        for (int i = 0; i < ale.size(); i++) {
            if (P_initial == 1) {
                subdivision.add(new Path());
            } else {
                subdivision.add(new Path());
                subdivision.get(i).alv.add(aln.get(ale.get(i).sourceNodeInd));
                subdivision.get(i).alv.add(aln.get(ale.get(i).targetNodeInd));
            }
        }
    }

    void initializeCompatibilityLists() {
        for (int i = 0; i < ale.size(); i++) {
            compatibility.add(new ArrayList<Edge>());
        }
    }

    ArrayList<Edge> filterSelfLoops(ArrayList<Edge> edgeList) {
        ArrayList<Edge> filteredEdgeList = new ArrayList<>();
        for (int i = 0; i < edgeList.size(); i++) {
            if (aln.get(edgeList.get(i).sourceNodeInd).x != aln.get(edgeList.get(i).targetNodeInd).x ||
                    aln.get(edgeList.get(i).sourceNodeInd).y != aln.get(edgeList.get(i).targetNodeInd).y) {
                filteredEdgeList.add(edgeList.get(i));
            }
        }
        return filteredEdgeList;
    }

    
}
