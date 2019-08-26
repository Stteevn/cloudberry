package algorithm;

import models.EdgeVector;
import models.Path;
import models.Point;

import java.util.ArrayList;

public class ForceBundling {

    ArrayList<Point> dataNodes;
    ArrayList<EdgeVector> dataEdges;
    ArrayList<Path> subdivisionPoints = new ArrayList<>();
    ArrayList<ArrayList<Integer>> compatibilityList = new ArrayList<>();
    final double K = 0.1;
    double S_initial = 0.02;
    final int P_initial = 1;
    final int P_rate = 2;
    final double C = 3;
    final double I_initial = 50;
    final double I_rate = 2.0 / 3.0;
    final double compatibility_threshold = 0.6;
    final double eps = 1e-6;
    private int isolatedEdgesCnt = 0;

    public ForceBundling(ArrayList<Point> dataNodes, ArrayList<EdgeVector> dataEdges) {
        this.dataNodes = dataNodes;
        this.dataEdges = dataEdges;
    }

    public int getIsolatedEdgesCnt() {
        return isolatedEdgesCnt;
    }

    public void setS(int zoom) {
        S_initial = Math.max(0.025 - zoom * 0.0025, 0.001);
    }

    double vectorDotProduct(Point p, Point q) {
        return p.getX() * q.getX() + p.getY() * q.getY();
    }

    Point edgeAsVector(EdgeVector e) {
        return new Point(dataNodes.get(e.getTargetNodeInd()).getX() - dataNodes.get(e.getSourceNodeInd()).getX(), dataNodes.get(e.getTargetNodeInd()).getY() - dataNodes.get(e.getSourceNodeInd()).getY());
    }

    double edgeLength(EdgeVector e) {
        if (Math.abs(dataNodes.get(e.getSourceNodeInd()).getX() - dataNodes.get(e.getTargetNodeInd()).getX()) < eps &&
                Math.abs(dataNodes.get(e.getSourceNodeInd()).getY() - dataNodes.get(e.getTargetNodeInd()).getY()) < eps) {
            return eps;
        }
        return Math.sqrt(Math.pow(dataNodes.get(e.getSourceNodeInd()).getX() - dataNodes.get(e.getTargetNodeInd()).getX(), 2) +
                Math.pow(dataNodes.get(e.getSourceNodeInd()).getY() - dataNodes.get(e.getTargetNodeInd()).getY(), 2));
    }

    // v1 for source, v2 for target
    double customEdgeLength(Point v1, Point v2) {
        return Math.sqrt(Math.pow(v1.getX() - v2.getX(), 2) + Math.pow(v1.getY() - v2.getY(), 2));
    }

    Point edgeMidPoint(EdgeVector e) {
        double midX = (dataNodes.get(e.getSourceNodeInd()).getX() + dataNodes.get(e.getTargetNodeInd()).getX()) / 2.0;
        double midY = (dataNodes.get(e.getSourceNodeInd()).getY() + dataNodes.get(e.getTargetNodeInd()).getY()) / 2.0;
        return new Point(midX, midY);
    }

    double euclideanDistance(Point p, Point q) {
        return Math.sqrt(Math.pow(p.getX() - q.getX(), 2) + Math.pow(p.getY() - q.getY(), 2));
    }

    double computeDividedEdgeLength(int e_ind) {
        double length = 0;
        for (int i = 1; i < subdivisionPoints.get(e_ind).getAlv().size(); i++) {
            double segmentLength = euclideanDistance(subdivisionPoints.get(e_ind).getAlv().get(i), subdivisionPoints.get(e_ind).getAlv().get(i - 1));
            length += segmentLength;
        }
        return length;
    }

    // q1 for source and q2 for target
    Point projectPointOnLine(Point p, Point q1, Point q2) {
        double L = Math.sqrt(Math.pow(q2.getX() - q1.getX(), 2) + Math.pow(q2.getY() - q1.getY(), 2));
        double r = ((q1.getY() - p.getY()) * (q1.getY() - q2.getY()) - (q1.getX() - p.getX()) * (q2.getX() - q1.getX())) / (Math.pow(L, 2));
        double x = q1.getX() + r * (q2.getX() - q1.getX());
        double y = q1.getY() + r * (q2.getY() - q1.getY());
        return new Point(x, y);
    }

    void initializeEdgeSubdivisions() {
        for (int i = 0; i < dataEdges.size(); i++) {
            if (P_initial == 1) {
                subdivisionPoints.add(new Path());
            } else {
                subdivisionPoints.add(new Path());
                subdivisionPoints.get(i).getAlv().add(dataNodes.get(dataEdges.get(i).getSourceNodeInd()));
                subdivisionPoints.get(i).getAlv().add(dataNodes.get(dataEdges.get(i).getTargetNodeInd()));
            }
        }
    }

    void initializeCompatibilityLists() {
        for (int i = 0; i < dataEdges.size(); i++) {
            compatibilityList.add(new ArrayList<>());
        }
    }

    Point applySpringForce(int e_ind, int i, double kP) {
        if (subdivisionPoints.get(e_ind).getAlv().size() <= 2) {
            return new Point(0, 0);
        }
        Point prev = subdivisionPoints.get(e_ind).getAlv().get(i - 1);
        Point next = subdivisionPoints.get(e_ind).getAlv().get(i + 1);
        Point crnt = subdivisionPoints.get(e_ind).getAlv().get(i);
        double x = prev.getX() - crnt.getX() + next.getX() - crnt.getX();
        double y = prev.getY() - crnt.getY() + next.getY() - crnt.getY();
        x *= kP;
        y *= kP;
        return new Point(x, y);
    }

    Point applyElectrostaticForce(int e_ind, int i) {
        Point sumOfForces = new Point(0, 0);
        ArrayList<Integer> compatibleEdgeList = compatibilityList.get(e_ind);
        for (int oe = 0; oe < compatibleEdgeList.size(); oe++) {
            double x = subdivisionPoints.get(compatibleEdgeList.get(oe)).getAlv().get(i).getX() - subdivisionPoints.get(e_ind).getAlv().get(i).getX();
            double y = subdivisionPoints.get(compatibleEdgeList.get(oe)).getAlv().get(i).getY() - subdivisionPoints.get(e_ind).getAlv().get(i).getY();
            Point force = new Point(x, y);
            if ((Math.abs(force.getX()) > eps || Math.abs(force.getY()) > eps)) {
                Point source = subdivisionPoints.get(compatibleEdgeList.get(oe)).getAlv().get(i);
                Point target = subdivisionPoints.get(e_ind).getAlv().get(i);
                double diff = customEdgeLength(source, target);
                sumOfForces.setX(sumOfForces.getX() + force.getX() / diff);
                sumOfForces.setY(sumOfForces.getY() + force.getY() / diff);
            }
        }
        return sumOfForces;
    }

    ArrayList<Point> applyResultingForcesOnSubdivisionPoints(int e_ind, int P, double S) {
        double kP = K / (edgeLength(dataEdges.get(e_ind)) * (P + 1));
        ArrayList<Point> resultingForcesForSubdivisionPoints = new ArrayList<>();
        resultingForcesForSubdivisionPoints.add(new Point(0, 0));
        for (int i = 1; i < P + 1; i++) {
            Point resultingForce = new Point(0, 0);
            Point springForce = applySpringForce(e_ind, i, kP);
            Point electrostaticForce = applyElectrostaticForce(e_ind, i);
            resultingForce.setX(S * (springForce.getX() + electrostaticForce.getX()));
            resultingForce.setY(S * (springForce.getY() + electrostaticForce.getY()));
            resultingForcesForSubdivisionPoints.add(resultingForce);
        }
        resultingForcesForSubdivisionPoints.add(new Point(0, 0));
        return resultingForcesForSubdivisionPoints;
    }

    void updateEdgeDivisions(int P) {
        for (int e_ind = 0; e_ind < dataEdges.size(); e_ind++) {
            if (P == 1) {
                subdivisionPoints.get(e_ind).getAlv().add(dataNodes.get(dataEdges.get(e_ind).getSourceNodeInd()));
                subdivisionPoints.get(e_ind).getAlv().add(edgeMidPoint(dataEdges.get(e_ind)));
                subdivisionPoints.get(e_ind).getAlv().add(dataNodes.get(dataEdges.get(e_ind).getTargetNodeInd()));
            } else {
                double dividedEdgeLength = computeDividedEdgeLength(e_ind);
                double segmentLength = dividedEdgeLength / (P + 1);
                double currentSegmentLength = segmentLength;
                ArrayList<Point> newDivisionPoints = new ArrayList<>();
                newDivisionPoints.add(dataNodes.get(dataEdges.get(e_ind).getSourceNodeInd()));
                for (int i = 1; i < subdivisionPoints.get(e_ind).getAlv().size(); i++) {
                    double oldSegmentLength = euclideanDistance(subdivisionPoints.get(e_ind).getAlv().get(i), subdivisionPoints.get(e_ind).getAlv().get(i - 1));
                    while (oldSegmentLength > currentSegmentLength) {
                        double percentPosition = currentSegmentLength / oldSegmentLength;
                        double newDivisionPointX = subdivisionPoints.get(e_ind).getAlv().get(i - 1).getX();
                        double newDivisionPointY = subdivisionPoints.get(e_ind).getAlv().get(i - 1).getY();
                        newDivisionPointX += percentPosition * (subdivisionPoints.get(e_ind).getAlv().get(i).getX() - subdivisionPoints.get(e_ind).getAlv().get(i - 1).getX());
                        newDivisionPointY += percentPosition * (subdivisionPoints.get(e_ind).getAlv().get(i).getY() - subdivisionPoints.get(e_ind).getAlv().get(i - 1).getY());
                        newDivisionPoints.add(new Point(newDivisionPointX, newDivisionPointY));
                        oldSegmentLength -= currentSegmentLength;
                        currentSegmentLength = segmentLength;
                    }
                    currentSegmentLength -= oldSegmentLength;
                }
                newDivisionPoints.add(dataNodes.get(dataEdges.get(e_ind).getTargetNodeInd()));
                subdivisionPoints.get(e_ind).setAlv(newDivisionPoints);
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
        Point midP = new Point(
                (dataNodes.get(P.getSourceNodeInd()).getX() + dataNodes.get(P.getTargetNodeInd()).getX()) / 2.0,
                (dataNodes.get(P.getSourceNodeInd()).getY() + dataNodes.get(P.getTargetNodeInd()).getY()) / 2.0
        );
        Point midQ = new Point(
                (dataNodes.get(Q.getSourceNodeInd()).getX() + dataNodes.get(Q.getTargetNodeInd()).getX()) / 2.0,
                (dataNodes.get(Q.getSourceNodeInd()).getY() + dataNodes.get(Q.getTargetNodeInd()).getY()) / 2.0
        );
        return lavg / (lavg + euclideanDistance(midP, midQ));
    }

    double edgeVisibility(EdgeVector P, EdgeVector Q) {
        Point I0 = projectPointOnLine(dataNodes.get(Q.getSourceNodeInd()), dataNodes.get(P.getSourceNodeInd()), dataNodes.get(P.getTargetNodeInd()));
        Point I1 = projectPointOnLine(dataNodes.get(Q.getTargetNodeInd()), dataNodes.get(P.getSourceNodeInd()), dataNodes.get(P.getTargetNodeInd()));
        Point midI = new Point(
                (I0.getX() + I1.getX()) / 2.0,
                (I0.getY() + I1.getY()) / 2.0
        );
        Point midP = new Point(
                (dataNodes.get(P.getSourceNodeInd()).getX() + dataNodes.get(P.getTargetNodeInd()).getX()) / 2.0,
                (dataNodes.get(P.getSourceNodeInd()).getY() + dataNodes.get(P.getTargetNodeInd()).getY()) / 2.0
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
            if (compatibilityList.get(e).isEmpty()) {
                isolatedEdgesCnt++;
            }
        }
    }

    public ArrayList<Path> forceBundle() {
        double S = S_initial;
        double I = I_initial;
        int P = P_initial;
        initializeEdgeSubdivisions();
        initializeCompatibilityLists();
        updateEdgeDivisions(P);
        computeCompatibilityLists();
        for (int cycle = 0; cycle < C; cycle++) {
            for (int iteration = 0; iteration < I; iteration++) {
                ArrayList<ArrayList<Point>> forces = new ArrayList<>();
                for (int edge = 0; edge < dataEdges.size(); edge++) {
                    forces.add(applyResultingForcesOnSubdivisionPoints(edge, P, S));
                }
                for (int e = 0; e < dataEdges.size(); e++) {
                    for (int i = 0; i < P + 1; i++) {
                        subdivisionPoints.get(e).getAlv().get(i).setX(subdivisionPoints.get(e).getAlv().get(i).getX() + forces.get(e).get(i).getX());
                        subdivisionPoints.get(e).getAlv().get(i).setY(subdivisionPoints.get(e).getAlv().get(i).getY() + forces.get(e).get(i).getY());
                    }
                }
            }
            S = S / 2;
            P = P * P_rate;
            I = I_rate * I;
            updateEdgeDivisions(P);
        }
        return subdivisionPoints;
    }
}
