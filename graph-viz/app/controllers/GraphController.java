package controllers;

import Utils.DatabaseUtils;
import Utils.PropertiesUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import models.*;
import play.mvc.*;
import actors.BundleActor;

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

    private PointCluster pointCluster = new PointCluster(0, 17);
    private IKmeans iKmeans;
    private Kmeans kmeans;
    // configuration properties
    private Properties configProps;
    private Set<Edge> edgeSet = new HashSet<>();
    // indicates the sending process is completed
    private final String finished = "Y";
    // indicates the sending process is not completed
    private final String unfinished = "N";

    /**
     * An action that renders an HTML page with a welcome message. The configuration
     * in the <code>routes</code> file means that this method will be called when
     * the application receives a <code>GET</code> request with a path of
     * <code>/</code>.
     */

    public void dispatcher(String query, BundleActor bundleActor) {
        if (query.equals("")) {
//            System.out.println("Heartbeat detected.");
            return;
        }
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode;
        int option = -1;
        try {
            jsonNode = objectMapper.readTree(query);
            option = Integer.parseInt(jsonNode.get("option").asText());
        } catch (IOException e) {
            e.printStackTrace();
        }
        switch (option) {
            case 0:
                getData(query, bundleActor);
                break;
            case 1:
                cluster(query, bundleActor);
                break;
            case 2:
                edgeCluster(query, bundleActor);
                break;
            default:
                System.err.println("Internal error: no option included");
                break;
        }
    }

    public void getData(String query, BundleActor bundleActor) {
        // parse the json and initialize the variables
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = null;
        try {
            jsonNode = objectMapper.readTree(query);
        } catch (IOException e) {
            e.printStackTrace();
        }
        int clusteringAlgo = Integer.parseInt(jsonNode.get("clusteringAlgo").asText());
        String timestamp = jsonNode.get("timestamp").asText();

        String endDate = null;
        query = jsonNode.get("query").asText();
        if (jsonNode.has("date")) {
            endDate = jsonNode.get("date").asText();
        }
        String json = "";
        ObjectNode objectNode = objectMapper.createObjectNode();
        try {
            Edge.set_epsilon(9);
            objectNode = queryResult(query, endDate, clusteringAlgo, timestamp, objectNode);
            objectNode.put("option", 0);
            json = objectNode.toString();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        bundleActor.returnData(json);
    }

    private ObjectNode queryResult(String query, String endDate, int clusteringAlgo, String timestamp, ObjectNode objectNode) {
        String firstDate = null;
        String lastDate = null;
        int queryPeriod = 0;
        try {
            configProps = PropertiesUtil.loadProperties(configProps);
            firstDate = configProps.getProperty("firstDate");
            lastDate = configProps.getProperty("lastDate");
            queryPeriod = Integer.parseInt(configProps.getProperty("queryPeriod"));
        } catch (IOException ex) {
            System.out.println("The config.properties file does not exist, default properties loaded.");
        }

        getData(query, endDate, clusteringAlgo, timestamp, objectNode, firstDate, lastDate, queryPeriod);
        return objectNode;
    }

    private void getData(String query, String endDate, int clusteringAlgo, String timestamp, ObjectNode objectNode, String firstDate, String lastDate, int queryPeriod) {
        Connection conn;
        PreparedStatement state;
        ResultSet resultSet;
        try {
            conn = DatabaseUtils.getConnection();

            String date = getDate(endDate, firstDate);
            String start = date;
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
            Calendar c = incrementCalendar(queryPeriod, date, sdf);
            date = sdf.format(c.getTime());
            Calendar lastDateCalendar = Calendar.getInstance();
            lastDateCalendar.setTime(sdf.parse(lastDate));

            bindFields(objectNode, timestamp, date, c, lastDateCalendar);
            state = prepareState(query, conn, date, start);
            resultSet = state.executeQuery();
            if (clusteringAlgo == 0) {
                loadHGC(resultSet);
            } else if (clusteringAlgo == 1) {
                loadIKmeans(resultSet);
            } else if (clusteringAlgo == 2) {
                start = firstDate;
                date = lastDate;
                c.setTime(sdf.parse(date));
                bindFields(objectNode, timestamp, date, c, lastDateCalendar);
                state = prepareState(query, conn, date, start);
                long beforequery = System.currentTimeMillis();
                resultSet = state.executeQuery();
                long queryTime = System.currentTimeMillis() - beforequery;
                System.out.println("query time: " + queryTime + "ms");
                loadKmeans(resultSet);
            }
            resultSet.close();
            state.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
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

    private PreparedStatement prepareState(String query, Connection conn, String date, String start) throws SQLException {
        PreparedStatement state;
        String searchQuery = "select from_longitude, from_latitude, to_longitude, to_latitude "
                + "from replytweets where ( to_tsvector('english', from_text) @@ to_tsquery( ? ) or "
                + "to_tsvector('english', to_text) "
                + "@@ to_tsquery( ? )) AND to_create_at::timestamp > TO_TIMESTAMP( ? , 'yyyymmddhh24miss') "
                + "AND to_create_at::timestamp <= TO_TIMESTAMP( ? , 'yyyymmddhh24miss');";
        state = conn.prepareStatement(searchQuery);
        state.setString(1, query);
        state.setString(2, query);
        state.setString(3, start);
        state.setString(4, date);
        return state;
    }

    private void bindFields(ObjectNode objectNode, String timestamp, String date, Calendar c, Calendar lastDateCalendar) {
        objectNode.put("date", date);
        objectNode.put("timestamp", timestamp);
        if (!c.before(lastDateCalendar)) {
            System.out.println(finished + date);
            objectNode.put("flag", finished);
        } else {
            System.out.println(unfinished + date);
            objectNode.put("flag", unfinished);
        }
    }


    private Calendar incrementCalendar(int queryPeriod, String date, SimpleDateFormat sdf) throws ParseException {
        Calendar c = Calendar.getInstance();
        c.setTime(sdf.parse(date));
        c.add(Calendar.HOUR, queryPeriod);  // number of days to add
        return c;
    }

    private String getDate(String endDate, String firstDate) {
        String date;
        if (endDate == null) {
            date = firstDate;  // Start date
        } else {
            date = endDate;
        }
        return date;
    }

    public void cluster(String query, BundleActor bundleActor) {
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
        objectNode.put("pointsCnt", pointsCnt);
        objectNode.put("clustersCnt", clustersCnt);
        bundleActor.returnData(objectNode.toString());
    }

    public void edgeCluster(String query, BundleActor bundleActor) {
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
        objectMapper = new ObjectMapper();
        ObjectNode objectNode = objectMapper.createObjectNode();
        ArrayNode arrayNode = objectMapper.createArrayNode();
        int edgesCnt = 0;
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
        objectNode.put("option", 2);
        objectNode.put("timestamp", timestamp);
        bundleActor.returnData(objectNode.toString());
    }

    private void getKmeansEdges(ObjectMapper objectMapper, int zoom, int bundling, int clustering, ObjectNode objectNode, ArrayNode arrayNode, boolean b, HashMap<Point, Integer> parents, ArrayList<double[]> center) {
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
                    Point fromPoint = new Point(fromLongitude, fromLatitude);
                    Point toPoint = new Point(toLongitude, toLatitude);
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
        ArrayList<Vector> dataNodes = new ArrayList<>();
        ArrayList<EdgeVector> dataEdges = new ArrayList<>();
        ArrayList<Integer> closeEdgeList = new ArrayList<>();
        for (Map.Entry<Edge, Integer> entry : edges.entrySet()) {
            Edge edge = entry.getKey();
            if (sqDistance(edge.getFromLongitude(), edge.getFromLatitude(), edge.getToLongitude(), edge.getToLatitude()) <= 0.001)
                continue;
            dataNodes.add(new Vector(edge.getFromLongitude(), edge.getFromLatitude()));
            dataNodes.add(new Vector(edge.getToLongitude(), edge.getToLatitude()));
            dataEdges.add(new EdgeVector(dataNodes.size() - 2, dataNodes.size() - 1));
            closeEdgeList.add(entry.getValue());
        }
        ForceBundling forceBundling = new ForceBundling(dataNodes, dataEdges);
        forceBundling.setS(zoom);
        long beforeBundling = System.currentTimeMillis();
        ArrayList<Path> pathResult = forceBundling.forceBundle();
        long bundlingTime = System.currentTimeMillis() - beforeBundling;
        System.out.println("bundling time: " + bundlingTime + "ms");
        isolatedEdgesCnt = forceBundling.isolatedEdgesCnt;
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

    private double sqDistance(double x1, double y1, double x2, double y2) {
        return Math.pow(x1 - x2, 2) + Math.pow(y1 - y2, 2);
    }

}
