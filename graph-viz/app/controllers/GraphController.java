package controllers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import models.Cluster;
import models.PointCluster;
import play.mvc.*;

import java.sql.*;
import java.util.*;

/**
 * This controller contains an action to handle HTTP requests to the
 * application's home page.
 */

public class GraphController extends Controller {

    private PointCluster pointCluster = new PointCluster(0, 17);
    private Set<Edge> edgeSet = new HashSet<>();

    /**
     * An action that renders an HTML page with a welcome message. The configuration
     * in the <code>routes</code> file means that this method will be called when
     * the application receives a <code>GET</code> request with a path of
     * <code>/</code>.
     */

    public Result index(String query) {
        String json = "";
        long startIndex = System.currentTimeMillis();
        Connection conn;
        PreparedStatement state;
        ResultSet resultSet;
        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/graphtweet", "graphuser",
                    "graphuser");
            String searchQuery = "select " + "from_longitude, from_latitude, " + "to_longitude, to_latitude "
                    + "from replytweets where (" + "to_tsvector('english', from_text) " + "@@ to_tsquery( ? ) or "
                    + "to_tsvector('english', to_text) "
                    + "@@ to_tsquery( ? ))";
            state = conn.prepareStatement(searchQuery);
            state.setString(1, query);
            state.setString(2, query);
            resultSet = state.executeQuery();
            long endData = System.currentTimeMillis();
            ArrayList<Cluster> points = new ArrayList<>();
            while (resultSet.next()) {
                double fromLongitude = resultSet.getDouble("from_longitude");
                double fromLatitude = resultSet.getDouble("from_latitude");
                double toLongitude = resultSet.getDouble("to_longitude");
                double toLatitude = resultSet.getDouble("to_latitude");
                points.add(new Cluster(fromLongitude, fromLatitude));
                points.add(new Cluster(toLongitude, toLatitude));
                Edge currentEdge = new Edge(fromLatitude, fromLongitude, toLatitude, toLongitude);
                edgeSet.add(currentEdge);
            }
            long startParse = System.currentTimeMillis();
            ObjectMapper objectMapper = new ObjectMapper();
            SimpleModule module = new SimpleModule();
            module.addSerializer(Edge.class, new EdgeSerializer());
            objectMapper.registerModule(module);
            json = objectMapper.writeValueAsString(edgeSet);
            long startCluster = System.currentTimeMillis();
            pointCluster.load(points);
            long endIndex = System.currentTimeMillis();
            System.out.println("Total time in executing query in database is " + (endData - startIndex) + " ms");
            System.out.println("Total time in parsing data from Json is " + (endIndex - startParse) + " ms");
            System.out.printf("Total time in clustering points is %d ms\n", endIndex - startCluster);
            System.out.println("Total time in running index() is " + (endIndex - startIndex) + " ms");
            System.out.println("Total number of records " + edgeSet.size());
            resultSet.close();
            state.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ok(json);
    }

    private HashMap<Integer, Edge> queryResult(String query, double lowerLongitude, double upperLongitude, double lowerLatitude,
                                               double upperLatitude) {
        Connection conn;
        PreparedStatement state;
        ResultSet resultSet;
        HashMap<Integer, Edge> edges = new HashMap<>();
        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/graphtweet", "graphuser",
                    "graphuser");
            String searchQuery = "select " + "from_longitude, from_latitude, " + "to_longitude, to_latitude "
                    + "from replytweets where (" + "to_tsvector('english', from_text) " + "@@ to_tsquery( ? ) or "
                    + "to_tsvector('english', to_text) "
                    + "@@ to_tsquery( ? )) and ((from_longitude between ? AND ? AND from_latitude between ? AND ?) OR"
                    + " (to_longitude between ? AND ? AND to_latitude between ? AND ?)) AND sqrt(pow(from_latitude - to_latitude, 2) + pow(from_longitude - to_longitude, 2)) >= ?;";
            state = conn.prepareStatement(searchQuery);
            state.setString(1, query);
            state.setString(2, query);
            state.setDouble(3, lowerLongitude);
            state.setDouble(4, upperLongitude);
            state.setDouble(5, lowerLatitude);
            state.setDouble(6, upperLatitude);
            state.setDouble(7, lowerLongitude);
            state.setDouble(8, upperLongitude);
            state.setDouble(9, lowerLatitude);
            state.setDouble(10, upperLatitude);
            state.setDouble(11, Math.sqrt(Math.pow(upperLongitude - lowerLongitude, 2) + Math.pow(upperLatitude - lowerLatitude, 2)) / 30);
            System.out.println(state.toString());
            resultSet = state.executeQuery();
            while (resultSet.next()) {
                double fromLongitude = resultSet.getDouble("from_longitude");
                double fromLatitude = resultSet.getDouble("from_latitude");
                double toLongitude = resultSet.getDouble("to_longitude");
                double toLatitude = resultSet.getDouble("to_latitude");

                Edge currentEdge = new Edge(fromLatitude, fromLongitude, toLatitude, toLongitude, 1);
                if (edges.containsKey(currentEdge.getBlock())) {
                    Edge oldEdge = edges.get(currentEdge.getBlock());
                    edges.remove(currentEdge.getBlock());
                    edges.put(currentEdge.getBlock(), oldEdge.updateWeight(1));
                } else {
                    edges.put(currentEdge.getBlock(), currentEdge);
                }
            }
            resultSet.close();
            state.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return edges;
    }

    public Result cluster(double lowerLongitude, double lowerLatitude, double upperLongitude,
                          double upperLatitude, int zoom) {
        String json = "";
        if (pointCluster != null) {
            ObjectMapper objectMapper = new ObjectMapper();
            ArrayNode arrayNode = objectMapper.createArrayNode();
            ArrayList<Cluster> clusters = pointCluster.getClusters(new double[]{lowerLongitude, lowerLatitude, upperLongitude, upperLatitude}, zoom);
            for (Cluster cluster : clusters) {
                ObjectNode objectNode = objectMapper.createObjectNode();
                objectNode.putArray("coordinates").add(PointCluster.xLng(cluster.x())).add(PointCluster.yLat(cluster.y()));
                objectNode.put("size", cluster.getNumPoints());
                arrayNode.add(objectNode);
            }
            json = arrayNode.toString();
            System.out.printf("The number of points is %d\n", clusters.size());
        }
        return ok(json);
    }

    public Result edgeCluster(double lowerLongitude, double lowerLatitude, double upperLongitude,
                              double upperLatitude, int zoom, int bundling) {
        String json = "";
        ObjectMapper objectMapper = new ObjectMapper();
        ArrayNode arrayNode = objectMapper.createArrayNode();
        if (pointCluster != null) {
            ArrayList<Cluster> clusters = pointCluster.getClusters(new double[]{-180, -90, 180, 90}, zoom);
            HashMap<Edge, Integer> edges = new HashMap<>();
            HashMap<EdgeVector, Integer> edgeVectors = new HashMap<>();
            for (Edge edge : edgeSet) {
                int i;
                int j;
                for (i = 0; i < clusters.size(); i++) {
                    Cluster cluster = clusters.get(i);
                    if (sqDistance(cluster.x(), cluster.y(), PointCluster.lngX(edge.getFromLongitude()), PointCluster.latY(edge.getFromLatitude()))
                            <= Math.pow(pointCluster.getZoomRadius(zoom), 2)) {
                        break;
                    }
                }
                for (j = 0; j < clusters.size(); j++) {
                    Cluster cluster = clusters.get(j);
                    if (sqDistance(cluster.x(), cluster.y(), PointCluster.lngX(edge.getToLongitude()), PointCluster.latY(edge.getToLatitude()))
                            <= Math.pow(pointCluster.getZoomRadius(zoom), 2)) {
                        break;
                    }
                }
                Edge e;
                if (i >= clusters.size() || j >= clusters.size()) {
                    continue;
                } else if (i == j) {
                    continue;
                }
                double fromLongitude = PointCluster.xLng(clusters.get(i).x());
                double fromLatitude = PointCluster.yLat(clusters.get(i).y());
                double toLongitude = PointCluster.xLng(clusters.get(j).x());
                double toLatitude = PointCluster.yLat(clusters.get(j).y());
                if ((lowerLongitude <= fromLongitude && fromLongitude <= upperLongitude
                        && lowerLatitude <= fromLatitude && fromLatitude <= upperLatitude)
                        || (lowerLongitude <= toLongitude && toLongitude <= upperLongitude
                        && lowerLatitude <= toLatitude && toLatitude <= upperLatitude)) {
                    e = new Edge(fromLatitude, fromLongitude, toLatitude, toLongitude);
                    EdgeVector edgeVector = new EdgeVector(i, j);
                    if (edgeVectors.containsKey(edgeVector)) {
                        edgeVectors.put(edgeVector, edgeVectors.get(edgeVector) + 1);
                    } else {
                        edgeVectors.put(edgeVector, 1);
                    }
                    if (edges.containsKey(e)) {
                        edges.put(e, edges.get(e) + 1);
                    } else {
                        edges.put(e, 1);
                    }
                }
            }
            /*
            double[] counts = new double[clusters.size()];
            for (EdgeVector e : edgeVectors.keySet().toArray(new EdgeVector[0])) {
                counts[e.sourceNodeInd] += new Edge(clusters.get(e.sourceNodeInd).y, clusters.get(e.sourceNodeInd).x, clusters.get(e.targetNodeInd).y, clusters.get(e.targetNodeInd).x).getDegree();
                counts[e.targetNodeInd] += new Edge(clusters.get(e.targetNodeInd).y, clusters.get(e.targetNodeInd).x, clusters.get(e.sourceNodeInd).y, clusters.get(e.sourceNodeInd).x).getDegree();
            }
            */
            System.out.printf("The number of edges is %d\n", edges.size());
            if (bundling == 0) {
                for (Map.Entry<Edge, Integer> entry : edges.entrySet()) {
                    ObjectNode objectNode = objectMapper.createObjectNode();
                    objectNode.putArray("from").add(entry.getKey().getFromLongitude()).add(entry.getKey().getFromLatitude());
                    objectNode.putArray("to").add(entry.getKey().getToLongitude()).add(entry.getKey().getToLatitude());
                    objectNode.put("width", entry.getValue());
                    arrayNode.add(objectNode);
                }
                double variance = okBundle(edges.keySet().toArray(new Edge[0]));
                arrayNode.add(variance);
                json = arrayNode.toString();
            } else {
                ArrayList<Vector> dataNodes = new ArrayList<>();
                ArrayList<EdgeVector> dataEdges = new ArrayList<>();
                ArrayList<Integer> closeEdgeList = new ArrayList<>();
                for (Map.Entry<Edge, Integer> entry : edges.entrySet()) {
                    Edge edge = entry.getKey();
                    if (sqDistance(edge.getFromLongitude(), edge.getFromLatitude(), edge.getToLongitude(), edge.getToLatitude()) <= 1)
                        continue;
                    dataNodes.add(new Vector(edge.getFromLongitude(), edge.getFromLatitude()));
                    dataNodes.add(new Vector(edge.getToLongitude(), edge.getToLatitude()));
                    dataEdges.add(new EdgeVector(dataNodes.size() - 2, dataNodes.size() - 1));
                    closeEdgeList.add(entry.getValue());
                }
                ForceBundling forceBundling = new ForceBundling(dataNodes, dataEdges);
                ArrayList<Path> pathResult = forceBundling.forceBundle();
                ArrayNode pathJson = objectMapper.createArrayNode();
                int edgeNum = 0;
                for (Path path : pathResult) {
                    for (int j = 0; j < path.alv.size() - 1; j++) {
                        ObjectNode lineNode = objectMapper.createObjectNode();
                        ArrayNode fromArray = objectMapper.createArrayNode();
                        fromArray.add(path.alv.get(j).x);
                        fromArray.add(path.alv.get(j).y);
                        ArrayNode toArray = objectMapper.createArrayNode();
                        toArray.add(path.alv.get(j + 1).x);
                        toArray.add(path.alv.get(j + 1).y);
                        lineNode.putArray("from").addAll(fromArray);
                        lineNode.putArray("to").addAll(toArray);
                        lineNode.put("width", closeEdgeList.get(edgeNum));
                        pathJson.add(lineNode);
                    }
                    edgeNum++;
                }
                json = pathJson.toString();

            }
        }
        return ok(json);
    }

    private double sqDistance(double x1, double y1, double x2, double y2) {
        return Math.pow(x1 - x2, 2) + Math.pow(y1 - y2, 2);
    }


    private double okBundle(Edge[] edges) {
        /*
        double averageAngle = 0;
        for (Edge e : edges) {
            averageAngle += Math.abs(e.getDegree());
        }
        averageAngle /= edges.length;
        double variance = 0;
        for (Edge e : edges) {
            variance += Math.pow((Math.abs(e.getDegree()) - averageAngle), 2);
        }
        System.out.printf("Variance * number of edges is %.4f, ", variance);
        variance /= edges.length;
        System.out.printf("Variance of angle is %.4f\n", variance);
        re turn variance;
        */

        int count = 0;
        int tot = 0;
        for (int i = 0; i < edges.length; i++) {
            Edge e1 = edges[i];
            for (int j = i + 1; j < edges.length; j++) {
                Edge e2 = edges[j];
                if (e1.getFromLongitude() == e2.getFromLongitude() && e1.getFromLatitude() == e2.getFromLatitude()) {
                    continue;
                } else if (e1.getFromLongitude() == e2.getToLongitude() && e1.getFromLatitude() == e2.getToLatitude()) {
                    continue;
                } else if (e1.getToLongitude() == e2.getFromLongitude() && e1.getToLatitude() == e2.getFromLatitude()) {
                    continue;
                } else if (e1.getToLongitude() == e2.getToLongitude() && e1.getToLatitude() == e2.getToLatitude()) {
                    continue;
                }
                if (e1.cross(e2)) {
                    count++;
                }
                tot++;
            }
        }
        double percentage = count * 100.0 / tot;
        return percentage;
    }
}
