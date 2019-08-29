package controllers;

import algorithms.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import models.*;
import play.mvc.*;
import actors.BundleActor;
import utils.DatabaseUtils;

import java.io.IOException;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * This controller contains an action to handle HTTP requests to the
 * application's home page.
 */

public class GraphController extends Controller {

    // Indicates the sending process is completed
    private static final String finished = "Y";
    // Indicates the sending process is not completed
    private static final String unfinished = "N";
    // hierarchical structure for HGC algorithm
    private PointCluster pointCluster = new PointCluster(0, 17);
    private IKmeans iKmeans;
    private Kmeans kmeans;
    // Incremental edge data
    private Set<Edge> edgeSet = new HashSet<>();
    private BundleActor bundleActor;

    /**
     * Dispatcher for the request message.
     *
     * @param query       received query message
     * @param actor WebSocket actor to return response.
     */
    public void dispatcher(String query, BundleActor actor) {
        bundleActor = actor;
        // Heartbeat package handler
        // WebSocket will automatically close after several seconds
        // To keep the state, maintain WebSocket connection is a must
        if (query.equals("")) {
            return;
        }
        // Parse the request message with JSON structure
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode;
        // Option indicates the request type
        // 1: incremental data query
        // 2: point and cluster
        // 3: edge and bundled edge and tree cut
        // others: invalid
        int option = -1;
        try {
            jsonNode = objectMapper.readTree(query);
            option = Integer.parseInt(jsonNode.get("option").asText());
        } catch (IOException e) {
            System.err.println("Invalid Request received.");
            e.printStackTrace();
        }
        switch (option) {
            case 0:
                new IncrementalQuery().prepareIncremental(this, query);
                break;
            case 1:
                cluster(query);
                break;
            case 2:
                edgeCluster(query);
                break;
            default:
                System.err.println("Internal error: no option included");
                break;
        }
    }

    public void doIncrementalQuery(String query, String firstDate, String lastDate, Calendar endCalendar, Calendar lastDateCalendar,
                                   SimpleDateFormat dateFormat, String start, String end)
            throws SQLException, ParseException, ClassNotFoundException {
        Connection conn = DatabaseUtils.getConnection();
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode objectNode = objectMapper.createObjectNode();
        JsonNode jsonNode;
        int clusteringAlgo = -1;
        String timestamp = null;
        try {
            jsonNode = objectMapper.readTree(query);
            clusteringAlgo = Integer.parseInt(jsonNode.get("clusteringAlgo").asText());
            timestamp = jsonNode.get("timestamp").asText();
            query = jsonNode.get("query").asText();
        } catch (IOException e) {
            e.printStackTrace();
        }
        PreparedStatement state = DatabaseUtils.prepareStatement(query, conn, end, start);
        ResultSet resultSet = state.executeQuery();
        bindFields(objectNode, timestamp, end, endCalendar, lastDateCalendar);
        runCluster(query, clusteringAlgo, timestamp, objectNode, firstDate, lastDate, conn, state, resultSet, endCalendar, lastDateCalendar, dateFormat);
    }

    private void runCluster(String query, int clusteringAlgo, String timestamp, ObjectNode objectNode, String firstDate, String lastDate, Connection conn, PreparedStatement state, ResultSet resultSet, Calendar endCalendar, Calendar lastDateCalendar, SimpleDateFormat dateFormat) throws ParseException, SQLException {
        String start;
        String end;
        if (clusteringAlgo == 0) {
            loadHGC(resultSet);
        } else if (clusteringAlgo == 1) {
            loadIKmeans(resultSet);
        } else if (clusteringAlgo == 2) {
            start = firstDate;
            end = lastDate;
            endCalendar.setTime(dateFormat.parse(end));
            bindFields(objectNode, timestamp, end, endCalendar, lastDateCalendar);
            state = DatabaseUtils.prepareStatement(query, conn, end, start);
            resultSet = state.executeQuery();
            loadKmeans(resultSet);
        }
        objectNode.put("option", 0);
        String json = objectNode.toString();
        bundleActor.returnData(json);
        resultSet.close();
        state.close();
    }

    private List<double[]> loadKmeansData(ResultSet resultSet) throws SQLException {
        List<double[]> data = new ArrayList<>();
        while (resultSet.next()) {
            double fromLongitude = resultSet.getDouble("from_longitude");
            double fromLatitude = resultSet.getDouble("from_latitude");
            double toLongitude = resultSet.getDouble("to_longitude");
            double toLatitude = resultSet.getDouble("to_latitude");
            Edge currentEdge = new Edge(fromLatitude, fromLongitude, toLatitude, toLongitude);
            if (edgeSet.contains(currentEdge)) continue;
            data.add(new double[]{fromLongitude, fromLatitude});
            data.add(new double[]{toLongitude, toLatitude});
            edgeSet.add(currentEdge);
        }
        return data;
    }

    private void loadKmeans(ResultSet resultSet) throws SQLException {
        List<double[]> data = loadKmeansData(resultSet);
        if (kmeans == null) {
            kmeans = new Kmeans(17);
        }
        kmeans.setDataSet(data);
        kmeans.execute();
    }

    private void loadIKmeans(ResultSet resultSet) throws SQLException {
        List<double[]> data = loadKmeansData(resultSet);
        if (iKmeans == null) {
            iKmeans = new IKmeans(17);
            iKmeans.setDataSet(data);
            iKmeans.init();
        }
        iKmeans.loadBatchData(data);
    }

    private void loadHGC(ResultSet resultSet) throws SQLException {
        ArrayList<Cluster> points = new ArrayList<>();
        while (resultSet.next()) {
            double fromLongitude = resultSet.getDouble("from_longitude");
            double fromLatitude = resultSet.getDouble("from_latitude");
            double toLongitude = resultSet.getDouble("to_longitude");
            double toLatitude = resultSet.getDouble("to_latitude");
            Edge currentEdge = new Edge(fromLatitude, fromLongitude, toLatitude, toLongitude);
            if (edgeSet.contains(currentEdge)) continue;
            points.add(new Cluster(fromLongitude, fromLatitude));
            points.add(new Cluster(toLongitude, toLatitude));
            edgeSet.add(currentEdge);
        }
        pointCluster.load(points);
    }

    private static void bindFields(ObjectNode objectNode, String timestamp, String end, Calendar endCalendar, Calendar lastDateCalendar) {
        objectNode.put("date", end);
        objectNode.put("timestamp", timestamp);
        if (!endCalendar.before(lastDateCalendar)) {
            System.out.println(finished + end);
            objectNode.put("flag", finished);
        } else {
            System.out.println(unfinished + end);
            objectNode.put("flag", unfinished);
        }
    }

    public void cluster(String query) {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = null;
        try {
            jsonNode = objectMapper.readTree(query);
        } catch (IOException e) {
            e.printStackTrace();
        }
        double lowerLongitude = Double.parseDouble(jsonNode.get("lowerLongitude").asText());
        double upperLongitude = Double.parseDouble(jsonNode.get("upperLongitude").asText());
        double lowerLatitude = Double.parseDouble(jsonNode.get("lowerLatitude").asText());
        double upperLatitude = Double.parseDouble(jsonNode.get("upperLatitude").asText());
        int clustering = Integer.parseInt(jsonNode.get("clustering").asText());
        int clusteringAlgo = Integer.parseInt(jsonNode.get("clusteringAlgo").asText());
        String timestamp = jsonNode.get("timestamp").asText();
        int zoom = Integer.parseInt(jsonNode.get("zoom").asText());
        String json = "";
        int pointsCnt = 0;
        int clustersCnt = 0;
        int repliesCnt = edgeSet.size();
        if (clusteringAlgo == 0) {
            if (pointCluster != null) {
                objectMapper = new ObjectMapper();
                ArrayNode arrayNode = objectMapper.createArrayNode();
                ArrayList<Cluster> points = pointCluster.getClusters(new double[]{lowerLongitude, lowerLatitude, upperLongitude, upperLatitude}, 18);
                ArrayList<Cluster> clusters = pointCluster.getClusters(new double[]{lowerLongitude, lowerLatitude, upperLongitude, upperLatitude}, zoom);
                pointsCnt = points.size();
                clustersCnt = clusters.size();
                for (Cluster cluster : clusters) {
                    ObjectNode objectNode = objectMapper.createObjectNode();
                    objectNode.putArray("coordinates").add(PointCluster.xLng(cluster.x())).add(PointCluster.yLat(cluster.y()));
                    objectNode.put("size", cluster.getNumPoints());
                    arrayNode.add(objectNode);
                }
                json = arrayNode.toString();
                System.out.printf("The number of points is %d\n", clustersCnt);
            }
        } else if (clusteringAlgo == 1) {
            if (iKmeans != null) {
                if (clustering == 0) {
                    objectMapper = new ObjectMapper();
                    ArrayNode arrayNode = objectMapper.createArrayNode();
                    pointsCnt = iKmeans.pointsCnt;
                    clustersCnt = pointsCnt;
                    for (int i = 0; i < iKmeans.getK(); i++) {
                        for (int j = 0; j < iKmeans.getAllCluster().get(i).size(); j++) {
                            ObjectNode objectNode = objectMapper.createObjectNode();
                            objectNode.putArray("coordinates").add(iKmeans.getAllCluster().get(i).get(j)[0]).add(iKmeans.getAllCluster().get(i).get(j)[1]);
                            objectNode.put("size", 1);
                            arrayNode.add(objectNode);
                        }
                    }
                    json = arrayNode.toString();
                    System.out.printf("The number of points is %d\n", clustersCnt);
                } else {
                    objectMapper = new ObjectMapper();
                    ArrayNode arrayNode = objectMapper.createArrayNode();
                    pointsCnt = iKmeans.pointsCnt;
                    clustersCnt = iKmeans.getK();
                    for (int i = 0; i < iKmeans.getK(); i++) {
                        ObjectNode objectNode = objectMapper.createObjectNode();
                        objectNode.putArray("coordinates").add(iKmeans.getCenter().get(i)[0]).add(iKmeans.getCenter().get(i)[1]);
                        objectNode.put("size", iKmeans.getAllCluster().get(i).size());
                        arrayNode.add(objectNode);
                    }
                    json = arrayNode.toString();
                    System.out.printf("The number of points is %d\n", clustersCnt);
                }
            }
        } else if (clusteringAlgo == 2) {
            if (kmeans != null) {
                if (clustering == 0) {
                    objectMapper = new ObjectMapper();
                    ArrayNode arrayNode = objectMapper.createArrayNode();
                    pointsCnt = kmeans.getDataSetLength();
                    clustersCnt = pointsCnt;
                    for (int i = 0; i < kmeans.getDataSetLength(); i++) {
                        ObjectNode objectNode = objectMapper.createObjectNode();
                        objectNode.putArray("coordinates").add(kmeans.getDataSet().get(i)[0]).add(kmeans.getDataSet().get(i)[1]);
                        objectNode.put("size", 1);
                        arrayNode.add(objectNode);
                    }
                    json = arrayNode.toString();
                    System.out.printf("The number of points is %d\n", clustersCnt);
                } else {
                    objectMapper = new ObjectMapper();
                    ArrayNode arrayNode = objectMapper.createArrayNode();
                    pointsCnt = kmeans.getDataSetLength();
                    clustersCnt = kmeans.getK();
                    for (int i = 0; i < kmeans.getK(); i++) {
                        ObjectNode objectNode = objectMapper.createObjectNode();
                        objectNode.putArray("coordinates").add(kmeans.getCenter().get(i)[0]).add(kmeans.getCenter().get(i)[1]);
                        objectNode.put("size", kmeans.getCluster().get(i).size());
                        arrayNode.add(objectNode);
                    }
                    json = arrayNode.toString();
                    System.out.printf("The number of points is %d\n", clustersCnt);
                }
            }
        }
        ObjectNode objectNode = objectMapper.createObjectNode();
        objectNode.put("option", 1);
        objectNode.put("data", json);
        objectNode.put("timestamp", timestamp);
        objectNode.put("repliesCnt", repliesCnt);
        objectNode.put("pointsCnt", pointsCnt);
        objectNode.put("clustersCnt", clustersCnt);
        bundleActor.returnData(objectNode.toString());
    }

    private void edgeCluster(String query) {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = null;
        try {
            jsonNode = objectMapper.readTree(query);
        } catch (IOException e) {
            e.printStackTrace();
        }
        double lowerLongitude = Double.parseDouble(jsonNode.get("lowerLongitude").asText());
        double upperLongitude = Double.parseDouble(jsonNode.get("upperLongitude").asText());
        double lowerLatitude = Double.parseDouble(jsonNode.get("lowerLatitude").asText());
        double upperLatitude = Double.parseDouble(jsonNode.get("upperLatitude").asText());
        int clusteringAlgo = Integer.parseInt(jsonNode.get("clusteringAlgo").asText());
        String timestamp = jsonNode.get("timestamp").asText();
        int zoom = Integer.parseInt(jsonNode.get("zoom").asText());
        int bundling = Integer.parseInt(jsonNode.get("bundling").asText());
        int clustering = Integer.parseInt(jsonNode.get("clustering").asText());
        int treeCutting = Integer.parseInt(jsonNode.get("treeCut").asText());
        objectMapper = new ObjectMapper();
        ObjectNode objectNode = objectMapper.createObjectNode();
        ArrayNode arrayNode = objectMapper.createArrayNode();
        int edgesCnt = 0;
        int repliesCnt = edgeSet.size();
        if (clusteringAlgo == 0) {
            if (pointCluster != null) {
                HashMap<Edge, Integer> edges = new HashMap<>();
                if (clustering == 0) {
                    for (Edge edge : edgeSet) {
                        if (edges.containsKey(edge)) {
                            edges.put(edge, edges.get(edge) + 1);
                        } else {
                            edges.put(edge, 1);
                        }
                    }
                } else {
                    HashSet<Edge> externalEdgeSet = new HashSet<>();
                    HashSet<Cluster> externalCluster = new HashSet<>();
                    HashSet<Cluster> internalCluster = new HashSet<>();
                    for (Edge edge : edgeSet) {
                        Cluster fromCluster = pointCluster.parentCluster(new Cluster(PointCluster.lngX(edge.getFromLongitude()), PointCluster.latY(edge.getFromLatitude())), zoom);
                        Cluster toCluster = pointCluster.parentCluster(new Cluster(PointCluster.lngX(edge.getToLongitude()), PointCluster.latY(edge.getToLatitude())), zoom);
                        double fromLongitude = PointCluster.xLng(fromCluster.x());
                        double fromLatitude = PointCluster.yLat(fromCluster.y());
                        double toLongitude = PointCluster.xLng(toCluster.x());
                        double toLatitude = PointCluster.yLat(toCluster.y());
                        if ((lowerLongitude <= fromLongitude && fromLongitude <= upperLongitude
                                && lowerLatitude <= fromLatitude && fromLatitude <= upperLatitude)
                                && (lowerLongitude <= toLongitude && toLongitude <= upperLongitude
                                && lowerLatitude <= toLatitude && toLatitude <= upperLatitude)) {
                            Edge e = new Edge(fromLatitude, fromLongitude, toLatitude, toLongitude);
                            if (edges.containsKey(e)) {
                                edges.put(e, edges.get(e) + 1);
                            } else {
                                edges.put(e, 1);
                            }
                            internalCluster.add(fromCluster);
                            internalCluster.add(toCluster);
                        } else if ((lowerLongitude <= fromLongitude && fromLongitude <= upperLongitude
                                && lowerLatitude <= fromLatitude && fromLatitude <= upperLatitude)
                                || (lowerLongitude <= toLongitude && toLongitude <= upperLongitude
                                && lowerLatitude <= toLatitude && toLatitude <= upperLatitude)) {
                            if (lowerLongitude <= fromLongitude && fromLongitude <= upperLongitude
                                    && lowerLatitude <= fromLatitude && fromLatitude <= upperLatitude) {
                                externalCluster.add(toCluster);
                            } else {
                                externalCluster.add(fromCluster);
                            }
                            externalEdgeSet.add(edge);
                        }
                    }
                    TreeCut treeCutInstance = new TreeCut();
                    if (treeCutting == 1) {
                        treeCutInstance.treeCut(pointCluster, lowerLongitude, upperLongitude, lowerLatitude, upperLatitude, zoom, edges, externalEdgeSet, externalCluster, internalCluster);
                    } else {
                        treeCutInstance.nonTreeCut(pointCluster, zoom, edges, externalEdgeSet);
                    }

                }
                edgesCnt = edges.size();
                objectNode.put("edgesCnt", edgesCnt);
                if (bundling == 0) {
                    objectNode = noBundling(objectMapper, objectNode, arrayNode, edgesCnt, edges);
                } else {
                    runFDEB(objectMapper, zoom, objectNode, edges);
                }
            }
        } else if (clusteringAlgo == 1) {
            getKmeansEdges(objectMapper, zoom, bundling, clustering, objectNode, arrayNode, iKmeans != null, iKmeans.getParents(), iKmeans.getCenter());
        } else if (clusteringAlgo == 2) {
            getKmeansEdges(objectMapper, zoom, bundling, clustering, objectNode, arrayNode, kmeans != null, kmeans.getParents(), kmeans.getCenter());
        }
        objectNode.put("repliesCnt", repliesCnt);
        objectNode.put("option", 2);
        objectNode.put("timestamp", timestamp);
        bundleActor.returnData(objectNode.toString());
    }


    private void getKmeansEdges(ObjectMapper objectMapper, int zoom, int bundling, int clustering, ObjectNode objectNode, ArrayNode arrayNode, boolean b, HashMap<models.Point, Integer> parents, ArrayList<double[]> center) {
        int edgesCnt;
        if (b) {
            HashMap<Edge, Integer> edges = new HashMap<>();
            if (clustering == 0) {
                for (Edge edge : edgeSet) {
                    if (edges.containsKey(edge)) {
                        edges.put(edge, edges.get(edge) + 1);
                    } else {
                        edges.put(edge, 1);
                    }
                }
            } else {
                for (Edge edge : edgeSet) {
                    double fromLongitude = edge.getFromLongitude();
                    double fromLatitude = edge.getFromLatitude();
                    double toLongitude = edge.getToLongitude();
                    double toLatitude = edge.getToLatitude();
                    models.Point fromPoint = new models.Point(fromLongitude, fromLatitude);
                    models.Point toPoint = new models.Point(toLongitude, toLatitude);
                    int fromCluster = parents.get(fromPoint);
                    int toCluster = parents.get(toPoint);
                    Edge e = new Edge(center.get(fromCluster)[1],
                            center.get(fromCluster)[0],
                            center.get(toCluster)[1],
                            center.get(toCluster)[0]);
                    if (edges.containsKey(e)) {
                        edges.put(e, edges.get(e) + 1);
                    } else {
                        edges.put(e, 1);
                    }
                }
            }
            edgesCnt = edges.size();
            objectNode.put("edgesCnt", edgesCnt);
            if (bundling == 0) {
                noBundling(objectMapper, objectNode, arrayNode, edgesCnt, edges);
            } else {
                runFDEB(objectMapper, zoom, objectNode, edges);
            }
        }
    }

    private void runFDEB(ObjectMapper objectMapper, int zoom, ObjectNode objectNode, HashMap<Edge, Integer> edges) {
        int isolatedEdgesCnt;
        String json;
        ArrayList<Point> dataNodes = new ArrayList<>();
        ArrayList<EdgeVector> dataEdges = new ArrayList<>();
        ArrayList<Integer> closeEdgeList = new ArrayList<>();
        for (Map.Entry<Edge, Integer> entry : edges.entrySet()) {
            Edge edge = entry.getKey();
            if (sqDistance(edge.getFromLongitude(), edge.getFromLatitude(), edge.getToLongitude(), edge.getToLatitude()) <= 0.001)
                continue;
            dataNodes.add(new Point(edge.getFromLongitude(), edge.getFromLatitude()));
            dataNodes.add(new Point(edge.getToLongitude(), edge.getToLatitude()));
            dataEdges.add(new EdgeVector(dataNodes.size() - 2, dataNodes.size() - 1));
            closeEdgeList.add(entry.getValue());
        }
        ForceBundling forceBundling = new ForceBundling(dataNodes, dataEdges);
        forceBundling.setS(zoom);
        long beforeBundling = System.currentTimeMillis();
        ArrayList<Path> pathResult = forceBundling.forceBundle();
        long bundlingTime = System.currentTimeMillis() - beforeBundling;
        System.out.println("bundling time: " + bundlingTime + "ms");
        isolatedEdgesCnt = forceBundling.getIsolatedEdgesCnt();
        ArrayNode pathJson = objectMapper.createArrayNode();
        int edgeNum = 0;
        for (Path path : pathResult) {
            for (int j = 0; j < path.getAlv().size() - 1; j++) {
                ObjectNode lineNode = objectMapper.createObjectNode();
                ArrayNode fromArray = objectMapper.createArrayNode();
                fromArray.add(path.getAlv().get(j).getX());
                fromArray.add(path.getAlv().get(j).getY());
                ArrayNode toArray = objectMapper.createArrayNode();
                toArray.add(path.getAlv().get(j + 1).getX());
                toArray.add(path.getAlv().get(j + 1).getY());
                lineNode.putArray("from").addAll(fromArray);
                lineNode.putArray("to").addAll(toArray);
                lineNode.put("width", closeEdgeList.get(edgeNum));
                pathJson.add(lineNode);
            }
            edgeNum++;
        }
        json = pathJson.toString();
        objectNode.put("data", json);
        objectNode.put("isolatedEdgesCnt", isolatedEdgesCnt);
    }

    private ObjectNode noBundling(ObjectMapper objectMapper, ObjectNode objectNode, ArrayNode arrayNode, int edgesCnt, HashMap<Edge, Integer> edges) {
        int isolatedEdgesCnt;
        String json;
        isolatedEdgesCnt = edgesCnt;
        for (Map.Entry<Edge, Integer> entry : edges.entrySet()) {
            ObjectNode lineNode = objectMapper.createObjectNode();
            lineNode.putArray("from").add(entry.getKey().getFromLongitude()).add(entry.getKey().getFromLatitude());
            lineNode.putArray("to").add(entry.getKey().getToLongitude()).add(entry.getKey().getToLatitude());
            lineNode.put("width", entry.getValue());
            arrayNode.add(lineNode);
        }
        json = arrayNode.toString();
        objectNode.put("data", json);
        objectNode.put("isolatedEdgesCnt", isolatedEdgesCnt);
        return objectNode;
    }


    private double sqDistance(double x1, double y1, double x2, double y2) {
        return Math.pow(x1 - x2, 2) + Math.pow(y1 - y2, 2);
    }


}
