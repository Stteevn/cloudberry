package controllers;

import Utils.DatabaseUtils;
import Utils.PropertiesUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import models.Cluster;
import models.IKmeans;
import models.PointCluster;
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
            System.out.println("Heartbeat detected.");
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
            }
            else if (clusteringAlgo == 1) {
                loadIKmeans(resultSet);
            }
            resultSet.close();
            state.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void loadIKmeans(ResultSet resultSet) throws SQLException {
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

    private Connection getConnection() throws ClassNotFoundException, SQLException {
        Class.forName("org.postgresql.Driver");
        return DriverManager.getConnection("jdbc:postgresql://localhost:5432/graphtweet", "graphuser",
                "graphuser");
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
        }
        else if (clusteringAlgo == 1) {
            if (iKmeans != null) {
                objectMapper = new ObjectMapper();
                ArrayNode arrayNode = objectMapper.createArrayNode();
                pointsCnt = iKmeans.pointsCnt;
                clustersCnt = iKmeans.getK();
                for (int i = 0; i < iKmeans.getK(); i++) {
                    ObjectNode objectNode = objectMapper.createObjectNode();
                    objectNode.putArray("coordinates").add(iKmeans.center.get(i)[0]).add(iKmeans.center.get(i)[1]);
                    objectNode.put("size", iKmeans.allCluster.get(i).size());
                    arrayNode.add(objectNode);
                }
                json = arrayNode.toString();
                System.out.printf("The number of points is %d\n", clustersCnt);
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
        String timestamp = jsonNode.get("timestamp").asText();
        int zoom = Integer.parseInt(jsonNode.get("zoom").asText());
        int bundling = Integer.parseInt(jsonNode.get("bundling").asText());
        int clustering = Integer.parseInt(jsonNode.get("clustering").asText());
        String json = "";
        objectMapper = new ObjectMapper();
        ObjectNode objectNode = objectMapper.createObjectNode();
        ArrayNode arrayNode = objectMapper.createArrayNode();
        int edgesCnt = 0;
        int isolatedEdgesCnt = 0;
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
                for (Edge edge : edgeSet) {
                    Cluster fromCluster = pointCluster.parentCluster(new Cluster(PointCluster.lngX(edge.getFromLongitude()), PointCluster.latY(edge.getFromLatitude())), zoom);
                    Cluster toCluster = pointCluster.parentCluster(new Cluster(PointCluster.lngX(edge.getToLongitude()), PointCluster.latY(edge.getToLatitude())), zoom);
                    double fromLongitude = PointCluster.xLng(fromCluster.x());
                    double fromLatitude = PointCluster.yLat(fromCluster.y());
                    double toLongitude = PointCluster.xLng(toCluster.x());
                    double toLatitude = PointCluster.yLat(toCluster.y());
                    // TODO change the relationship from or to and
                    if ((lowerLongitude <= fromLongitude && fromLongitude <= upperLongitude
                            && lowerLatitude <= fromLatitude && fromLatitude <= upperLatitude)
                            || (lowerLongitude <= toLongitude && toLongitude <= upperLongitude
                            && lowerLatitude <= toLatitude && toLatitude <= upperLatitude)) {
                        Edge e = new Edge(fromLatitude, fromLongitude, toLatitude, toLongitude);
                        if (edges.containsKey(e)) {
                            edges.put(e, edges.get(e) + 1);
                        } else {
                            edges.put(e, 1);
                        }
                    }
                }
            }
            edgesCnt = edges.size();
            System.out.printf("The number of edges is %d\n", edges.size());
            if (bundling == 0) {
                isolatedEdgesCnt = edgesCnt;
                for (Map.Entry<Edge, Integer> entry : edges.entrySet()) {
                    objectNode = objectMapper.createObjectNode();
                    objectNode.putArray("from").add(entry.getKey().getFromLongitude()).add(entry.getKey().getFromLatitude());
                    objectNode.putArray("to").add(entry.getKey().getToLongitude()).add(entry.getKey().getToLatitude());
                    objectNode.put("width", entry.getValue());
                    arrayNode.add(objectNode);
                }
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

            }
        }
        objectNode.put("option", 2);
        objectNode.put("data", json);
        objectNode.put("timestamp", timestamp);
        objectNode.put("edgesCnt", edgesCnt);
        objectNode.put("isolatedEdgesCnt", isolatedEdgesCnt);
        bundleActor.returnData(objectNode.toString());
    }

    private double sqDistance(double x1, double y1, double x2, double y2) {
        return Math.pow(x1 - x2, 2) + Math.pow(y1 - y2, 2);
    }

}
