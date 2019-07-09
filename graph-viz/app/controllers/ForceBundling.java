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

    static ArrayList<Node> dataNodes = new ArrayList<>();
    static ArrayList<Edge> dataEdges = new ArrayList<>();
    static ArrayList<Path> subdivisionPoints = new ArrayList<>();
    static ArrayList<ArrayList<Integer>> compatibilityList = new ArrayList<>();
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
        return new Vector(dataNodes.get(e.targetNodeInd).x - dataNodes.get(e.sourceNodeInd).x, dataNodes.get(e.targetNodeInd).y - dataNodes.get(e.sourceNodeInd).y);
    }

    double edgeLength(Edge e) {
        if (Math.abs(dataNodes.get(e.sourceNodeInd).x - dataNodes.get(e.targetNodeInd).x) < eps &&
                Math.abs(dataNodes.get(e.sourceNodeInd).y - dataNodes.get(e.targetNodeInd).y) < eps) {
            return eps;
        }
        return Math.sqrt(Math.pow(dataNodes.get(e.sourceNodeInd).x - dataNodes.get(e.targetNodeInd).x, 2) +
                Math.pow(dataNodes.get(e.sourceNodeInd).y - dataNodes.get(e.targetNodeInd).y, 2));
    }

    // v1 for source, v2 for target
    double customEdgeLength(Node v1, Node v2) {
        return Math.sqrt(Math.pow(v1.x - v2.x, 2) + Math.pow(v1.y - v2.y, 2));
    }

    Vector edgeMidPoint(Edge e) {
        double midX = (dataNodes.get(e.sourceNodeInd).x + dataNodes.get(e.targetNodeInd).x) / 2.0;
        double midY = (dataNodes.get(e.sourceNodeInd).y + dataNodes.get(e.targetNodeInd).y) / 2.0;
        return new Vector(midX, midY);
    }

    double euclideanDistance(Node p, Node q) {
        return Math.sqrt(Math.pow(p.x - q.x, 2) + Math.pow(p.y - q.y, 2));
    }

    double computeDividedEdgeLength(int e_ind) {
        double length = 0;
        for (int i = 1; i < subdivisionPoints.get(e_ind).alv.size(); i++) {
            double segmentLength = euclideanDistance(subdivisionPoints.get(e_ind).alv.get(i), subdivisionPoints.get(e_ind).alv.get(i - 1));
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
        for (int i = 0; i < dataEdges.size(); i++) {
            if (P_initial == 1) {
                subdivisionPoints.add(new Path());
            } else {
                subdivisionPoints.add(new Path());
                subdivisionPoints.get(i).alv.add(dataNodes.get(dataEdges.get(i).sourceNodeInd));
                subdivisionPoints.get(i).alv.add(dataNodes.get(dataEdges.get(i).targetNodeInd));
            }
        }
    }

    void initializeCompatibilityLists() {
        for (int i = 0; i < dataEdges.size(); i++) {
            compatibilityList.add(new ArrayList<>());
        }
    }

    ArrayList<Edge> filterSelfLoops(ArrayList<Edge> edgeList) {
        ArrayList<Edge> filteredEdgeList = new ArrayList<>();
        for (int i = 0; i < edgeList.size(); i++) {
            if (dataNodes.get(edgeList.get(i).sourceNodeInd).x != dataNodes.get(edgeList.get(i).targetNodeInd).x ||
                    dataNodes.get(edgeList.get(i).sourceNodeInd).y != dataNodes.get(edgeList.get(i).targetNodeInd).y) {
                filteredEdgeList.add(edgeList.get(i));
            }
        }
        return filteredEdgeList;
    }

    Vector applySpringForce(int e_ind, int i, double kP) {
        Node prev = subdivisionPoints.get(e_ind).alv.get(i - 1);
        Node next = subdivisionPoints.get(e_ind).alv.get(i + 1);
        Node crnt = subdivisionPoints.get(e_ind).alv.get(i);
        double x = prev.x - crnt.x + next.x - crnt.x;
        double y = prev.y - crnt.y + next.y - crnt.y;
        x *= kP;
        y *= kP;
        return new Vector(x, y);
    }

    Vector applyElectrostaticForce(int e_ind, int i) {
        Vector sumOfForces = new Vector(0, 0);
        ArrayList<Integer> compatibleEdgeList = compatibilityList.get(e_ind);
        for (int oe = 0; oe < compatibleEdgeList.size(); oe++) {
            double x = subdivisionPoints.get(compatibleEdgeList.get(oe)).alv.get(i).x - subdivisionPoints.get(e_ind).alv.get(i).x;
            double y = subdivisionPoints.get(compatibleEdgeList.get(oe)).alv.get(i).y - subdivisionPoints.get(e_ind).alv.get(i).y;
            Vector force = new Vector(x, y);
            if ((Math.abs(force.x) > eps || Math.abs(force.y) > eps)) {
                Node source = subdivisionPoints.get(compatibleEdgeList.get(oe)).alv.get(i);
                Node target = subdivisionPoints.get(e_ind).alv.get(i);
                double diff = (1 / Math.pow(customEdgeLength(source, target), 1));
                sumOfForces.x += force.x * diff;
                sumOfForces.y += force.y * diff;
            }
        }
        return sumOfForces;
    }

    ArrayList<Vector> applyResultingForcesOnSubdivisionPoints(int e_ind, double P, double S) {
        double kP = K / (edgeLength(dataEdges.get(e_ind)) * (P + 1));
        ArrayList<Vector> resultingForcesForSubdivisionPoints = new ArrayList<>();
        resultingForcesForSubdivisionPoints.add(new Vector(0, 0));
        for (int i = 1; i < P + 1; i++) {
            Vector resultingForce = new Vector(0, 0);
            Vector springForce = applySpringForce(e_ind, i, kP);
            Vector electrostaticForce = applyElectrostaticForce(e_ind, i);
            resultingForce.x = S * (springForce.x + electrostaticForce.x);
            resultingForce.y = S * (springForce.y + electrostaticForce.y);
            resultingForcesForSubdivisionPoints.add(resultingForce);
        }
        resultingForcesForSubdivisionPoints.add(new Vector(0, 0));
        return resultingForcesForSubdivisionPoints;
    }

}
