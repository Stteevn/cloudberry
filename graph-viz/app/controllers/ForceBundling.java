package controllers;

import java.util.ArrayList;

class EdgeVector {
    int sourceNodeInd;
    int targetNodeInd;

    EdgeVector(int sourceNodeInd, int targetNodeInd) {
        this.sourceNodeInd = sourceNodeInd;
        this.targetNodeInd = targetNodeInd;
    }
}

class Vector {
    double x;
    double y;

    Vector() {
    }

    Vector(double x, double y) {
        this.x = x;
        this.y = y;
    }
}

class Path {
    ArrayList<Vector> alv;

    Path() {
        alv = new ArrayList<>();
    }
}

public class ForceBundling {

    ArrayList<Vector> dataNodes;
    ArrayList<EdgeVector> dataEdges;
    ArrayList<Path> subdivisionPoints = new ArrayList<>();
    ArrayList<ArrayList<Integer>> compatibilityList = new ArrayList<>();
    final double K = 0.1;
    final double S_initial = 0.3;
    final int P_initial = 1;
    final int P_rate = 2;
    final double C = 5;
    final double I_initial = 40;
    final double I_rate = 2.0 / 3.0;
    final double compatibility_threshold = 0.75;
    final double eps = 1e-6;
    Vector centerL;
    Vector centerR;
    // length of edges already dealt with
    int length;

    public ForceBundling(ArrayList<Vector> dataNodes, ArrayList<EdgeVector> dataEdges) {
        this.dataNodes = dataNodes;
        this.dataEdges = dataEdges;
        this.centerL = new Vector(0.0, 0.0);
        this.centerR = new Vector(0.0, 0.0);
        this.length = 0;
        for (int o = 0; o < dataEdges.size(); o++) {
            Vector s = dataNodes.get(dataEdges.get(o).sourceNodeInd);
            Vector t = dataNodes.get(dataEdges.get(o).targetNodeInd);
            Vector l = s.x < t.x ? s : t;
            Vector r = s.x < t.x ? t : s;
            this.centerL.x = (this.centerL.x * length + l.x) / (length + 1) * 1.0;
            this.centerL.y = (this.centerL.y * length + l.y) / (length + 1) * 1.0;
            this.centerR.x = (this.centerR.x * length + r.x) / (length + 1) * 1.0;
            this.centerR.y = (this.centerR.y * length + r.y) / (length + 1) * 1.0;

        }
    }

    public ForceBundling(ArrayList<Vector> dataNodes, ArrayList<EdgeVector> dataEdges, Vector cL, Vector cR, int length) {
        this.dataNodes = dataNodes;
        this.dataEdges = dataEdges;
        this.centerL = cL;
        this.centerR = cR;
        this.length = length;
        for (int o = 0; o < dataEdges.size(); o++) {
            Vector s = dataNodes.get(dataEdges.get(o).sourceNodeInd);
            Vector t = dataNodes.get(dataEdges.get(o).targetNodeInd);
            Vector l = s.x < t.x ? s : t;
            Vector r = s.x < t.x ? t : s;
            this.centerL.x = (this.centerL.x * length + l.x) / (length + 1) * 1.0;
            this.centerL.y = (this.centerL.y * length + l.y) / (length + 1) * 1.0;
            this.centerR.x = (this.centerR.x * length + r.x) / (length + 1) * 1.0;
            this.centerR.y = (this.centerR.y * length + r.y) / (length + 1) * 1.0;

        }
    }

    double vectorDotProduct(Vector p, Vector q) {
        return p.x * q.x + p.y * q.y;
    }

    Vector edgeAsVector(EdgeVector e) {
        return new Vector(dataNodes.get(e.targetNodeInd).x - dataNodes.get(e.sourceNodeInd).x, dataNodes.get(e.targetNodeInd).y - dataNodes.get(e.sourceNodeInd).y);
    }

    double edgeLength(EdgeVector e) {
        if (Math.abs(dataNodes.get(e.sourceNodeInd).x - dataNodes.get(e.targetNodeInd).x) < eps &&
                Math.abs(dataNodes.get(e.sourceNodeInd).y - dataNodes.get(e.targetNodeInd).y) < eps) {
            return eps;
        }
        return Math.sqrt(Math.pow(dataNodes.get(e.sourceNodeInd).x - dataNodes.get(e.targetNodeInd).x, 2) +
                Math.pow(dataNodes.get(e.sourceNodeInd).y - dataNodes.get(e.targetNodeInd).y, 2));
    }

    // v1 for source, v2 for target
    double customEdgeLength(Vector v1, Vector v2) {
        return Math.sqrt(Math.pow(v1.x - v2.x, 2) + Math.pow(v1.y - v2.y, 2));
    }

    Vector edgeMidPoint(EdgeVector e) {
        double midX = (dataNodes.get(e.sourceNodeInd).x + dataNodes.get(e.targetNodeInd).x) / 2.0;
        double midY = (dataNodes.get(e.sourceNodeInd).y + dataNodes.get(e.targetNodeInd).y) / 2.0;
        return new Vector(midX, midY);
    }

    double euclideanDistance(Vector p, Vector q) {
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

    Vector applySpringForce(int e_ind, int i, double kP) {
        if (subdivisionPoints.get(e_ind).alv.size() <= 2) {
            return new Vector(0, 0);
        }
        Vector prev = subdivisionPoints.get(e_ind).alv.get(i - 1);
        Vector next = subdivisionPoints.get(e_ind).alv.get(i + 1);
        Vector crnt = subdivisionPoints.get(e_ind).alv.get(i);
        double x = prev.x - crnt.x + next.x - crnt.x;
        double y = prev.y - crnt.y + next.y - crnt.y;
        x *= kP;
        y *= kP;
        return new Vector(x, y);
    }

    Vector applyElectrostaticForce(int e_ind, int i) {
        Vector sumOfForces = new Vector(0, 0);
        ArrayList<Integer> compatibleEdgeList = compatibilityList.get(e_ind);
        if(e_ind == 0) {
            weightApplyElectrostaticForce(this.length * 1000000000, e_ind, i, sumOfForces, compatibleEdgeList);
        }else{
            weightApplyElectrostaticForce(1, e_ind, i, sumOfForces, compatibleEdgeList);
        }

        return sumOfForces;
    }

    private void weightApplyElectrostaticForce(int weight, int e_ind, int i, Vector sumOfForces, ArrayList<Integer> compatibleEdgeList) {
        for (int oe = 0; oe < compatibleEdgeList.size(); oe++) {
            double x = subdivisionPoints.get(compatibleEdgeList.get(oe)).alv.get(i).x - subdivisionPoints.get(e_ind).alv.get(i).x;
            double y = subdivisionPoints.get(compatibleEdgeList.get(oe)).alv.get(i).y - subdivisionPoints.get(e_ind).alv.get(i).y;
            Vector force = new Vector(x, y);
            if ((Math.abs(force.x) > eps || Math.abs(force.y) > eps)) {
                Vector source =subdivisionPoints.get(compatibleEdgeList.get(oe)).alv.get(i);
                Vector target = subdivisionPoints.get(e_ind).alv.get(i);
                double diff = customEdgeLength(source, target);
                sumOfForces.x += weight * force.x / diff;
                sumOfForces.y += weight * force.y / diff;
            }
        }
    }

    ArrayList<Vector> applyResultingForcesOnSubdivisionPoints(int e_ind, int P, double S) {
        double kP = K / (edgeLength(dataEdges.get(e_ind)) * (P + 1));
        ArrayList<Vector> resultingForcesForSubdivisionPoints = new ArrayList<>();
        resultingForcesForSubdivisionPoints.add(new Vector(0, 0));
        for (int i = 1; i < P + 1; i++) {
            Vector resultingForce = new Vector(0, 0);
            Vector springForce = applySpringForce(e_ind, i, kP);
            Vector electrostaticForce = applyElectrostaticForce(e_ind, i);
            double flen = Math.sqrt(Math.pow(springForce.x + electrostaticForce.x, 2) + Math.pow(springForce.y + electrostaticForce.y, 2));
            if (flen > 1e-4) {
                resultingForce.x = S * (springForce.x + electrostaticForce.x) / flen;
                resultingForce.y = S * (springForce.y + electrostaticForce.y) / flen;
            }
            resultingForcesForSubdivisionPoints.add(resultingForce);
        }
        resultingForcesForSubdivisionPoints.add(new Vector(0, 0));
        return resultingForcesForSubdivisionPoints;
    }

    void updateEdgeDivisions(int P) {
        for (int e_ind = 0; e_ind < dataEdges.size(); e_ind++) {
            if (P == 1) {
                subdivisionPoints.get(e_ind).alv.add(dataNodes.get(dataEdges.get(e_ind).sourceNodeInd));
                subdivisionPoints.get(e_ind).alv.add(edgeMidPoint(dataEdges.get(e_ind)));
                subdivisionPoints.get(e_ind).alv.add(dataNodes.get(dataEdges.get(e_ind).targetNodeInd));
            } else {
                double dividedEdgeLength = computeDividedEdgeLength(e_ind);
                double segmentLength = dividedEdgeLength / (P + 1);
                double currentSegmentLength = segmentLength;
                ArrayList<Vector> newDivisionPoints = new ArrayList<>();
                newDivisionPoints.add(dataNodes.get(dataEdges.get(e_ind).sourceNodeInd));
                for (int i = 1; i < subdivisionPoints.get(e_ind).alv.size(); i++) {
                    double oldSegmentLength = euclideanDistance(subdivisionPoints.get(e_ind).alv.get(i), subdivisionPoints.get(e_ind).alv.get(i - 1));
                    while (oldSegmentLength > currentSegmentLength) {
                        double percentPosition = currentSegmentLength / oldSegmentLength;
                        double newDivisionPointX = subdivisionPoints.get(e_ind).alv.get(i - 1).x;
                        double newDivisionPointY = subdivisionPoints.get(e_ind).alv.get(i - 1).y;
                        newDivisionPointX += percentPosition * (subdivisionPoints.get(e_ind).alv.get(i).x - subdivisionPoints.get(e_ind).alv.get(i - 1).x);
                        newDivisionPointY += percentPosition * (subdivisionPoints.get(e_ind).alv.get(i).y - subdivisionPoints.get(e_ind).alv.get(i - 1).y);
                        newDivisionPoints.add(new Vector(newDivisionPointX, newDivisionPointY));
                        oldSegmentLength -= currentSegmentLength;
                        currentSegmentLength = segmentLength;
                    }
                    currentSegmentLength -= oldSegmentLength;
                }
                newDivisionPoints.add(dataNodes.get(dataEdges.get(e_ind).targetNodeInd));
                subdivisionPoints.get(e_ind).alv = newDivisionPoints;
            }
        }
    }

    double angleCompatibility(EdgeVector P, EdgeVector Q) {
        return Math.abs(vectorDotProduct(edgeAsVector(P), edgeAsVector(Q)) / (edgeLength(P) * edgeLength(Q)));
    }

    double scaleCompatibility(EdgeVector P, EdgeVector Q) {
        double lavg = (edgeLength(P) + edgeLength(Q)) / 2.0;
        return 2.0 / (lavg / Math.min(edgeLength(P), edgeLength(Q)) + Math.max(edgeLength(P), edgeLength(Q)) / lavg);
    }

    double positionCompatibility(EdgeVector P, EdgeVector Q) {
        double lavg = (edgeLength(P) + edgeLength(Q)) / 2.0;
        Vector midP = new Vector(
                (dataNodes.get(P.sourceNodeInd).x + dataNodes.get(P.targetNodeInd).x) / 2.0,
                (dataNodes.get(P.sourceNodeInd).y + dataNodes.get(P.targetNodeInd).y) / 2.0
        );
        Vector midQ = new Vector(
                (dataNodes.get(Q.sourceNodeInd).x + dataNodes.get(Q.targetNodeInd).x) / 2.0,
                (dataNodes.get(Q.sourceNodeInd).y + dataNodes.get(Q.targetNodeInd).y) / 2.0
        );
        return lavg / (lavg + euclideanDistance(midP, midQ));
    }

    double edgeVisibility(EdgeVector P, EdgeVector Q) {
        Vector I0 = projectPointOnLine(dataNodes.get(Q.sourceNodeInd), dataNodes.get(P.sourceNodeInd), dataNodes.get(P.targetNodeInd));
        Vector I1 = projectPointOnLine(dataNodes.get(Q.targetNodeInd), dataNodes.get(P.sourceNodeInd), dataNodes.get(P.targetNodeInd));
        Vector midI = new Vector(
                (I0.x + I1.x) / 2.0,
                (I0.y + I1.y) / 2.0
        );
        Vector midP = new Vector(
                (dataNodes.get(P.sourceNodeInd).x + dataNodes.get(P.targetNodeInd).x) / 2.0,
                (dataNodes.get(P.sourceNodeInd).y + dataNodes.get(P.targetNodeInd).y) / 2.0
        );
        return Math.max(0, 1 - 2 * euclideanDistance(midP, midI) / euclideanDistance(I0, I1));
    }

    double visibilityCompatibility(EdgeVector P, EdgeVector Q) {
        return Math.min(edgeVisibility(P, Q), edgeVisibility(Q, P));
    }

    double compatibilityScore(EdgeVector P, EdgeVector Q) {
        return (angleCompatibility(P, Q) * scaleCompatibility(P, Q) * positionCompatibility(P, Q) * visibilityCompatibility(P, Q));
    }

    boolean areCompatible(EdgeVector P, EdgeVector Q) {
        return (compatibilityScore(P, Q) > compatibility_threshold);
    }

    void computeCompatibilityLists() {
        for (int e = 0; e < dataEdges.size() - 1; e++) {
            for (int oe = e + 1; oe < dataEdges.size(); oe++) {
                if (areCompatible(dataEdges.get(e), dataEdges.get(oe))) {
                    compatibilityList.get(e).add(oe);
                    compatibilityList.get(oe).add(e);
                }
            }
        }
    }

    ForceBundling forceBundle() {
        double S = S_initial;
        double I = I_initial;
        int P = P_initial;
        initializeEdgeSubdivisions();
        initializeCompatibilityLists();
        updateEdgeDivisions(P);
        computeCompatibilityLists();
        for (int cycle = 0; cycle < C; cycle++) {
            for (int iteration = 0; iteration < I; iteration++) {
                ArrayList<ArrayList<Vector>> forces = new ArrayList<>();
                for (int edge = 0; edge < dataEdges.size(); edge++) {
                    forces.add(applyResultingForcesOnSubdivisionPoints(edge, P, S));
                }
                for (int e = 0; e < dataEdges.size(); e++) {
                    for (int i = 0; i < P + 1; i++) {
                        subdivisionPoints.get(e).alv.get(i).x += forces.get(e).get(i).x;
                        subdivisionPoints.get(e).alv.get(i).y += forces.get(e).get(i).y;
                    }
                }
            }
            S = S / 2;
            P = P * P_rate;
            I = I_rate * I;
            updateEdgeDivisions(P);
        }
        this.length += dataEdges.size();
        return this;
    }
}
