package controllers;

import Utils.PropertiesUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import models.Cluster;
import models.PointCluster;
import play.libs.Json;
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
    // configuration properties
    private Properties configProps;
    private Set<Edge> edgeSet = new HashSet<>();
    //    private ArrayList<Edge> allEdges;
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
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = null;
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
        double lowerLongitude = Double.parseDouble(jsonNode.get("lowerLongitude").asText());
        double upperLongitude = Double.parseDouble(jsonNode.get("upperLongitude").asText());
        double lowerLatitude = Double.parseDouble(jsonNode.get("lowerLatitude").asText());
        double upperLatitude = Double.parseDouble(jsonNode.get("upperLatitude").asText());
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
            objectNode = queryResult(query, endDate, lowerLongitude, upperLongitude, lowerLatitude, upperLatitude, timestamp, objectNode);
            objectNode.put("option", 0);
            json = objectNode.toString();
            System.out.println(json);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        bundleActor.returnData(json);
    }

    private ObjectNode queryResult(String query, String endDate, double lowerLongitude, double upperLongitude, double lowerLatitude,
                                   double upperLatitude, String timestamp, ObjectNode objectNode) {
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

        getData(query, endDate, lowerLongitude, upperLongitude, lowerLatitude, upperLatitude, timestamp, objectNode, firstDate, lastDate, queryPeriod);
        return objectNode;
    }

    private void getData(String query, String endDate, double lowerLongitude, double upperLongitude, double lowerLatitude, double upperLatitude, String timestamp, ObjectNode objectNode, String firstDate, String lastDate, int queryPeriod) {
        Connection conn;
        PreparedStatement state;
        ResultSet resultSet;
        try {
            conn = getConnection();

            String date = getDate(endDate, firstDate);
            String start = date;
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
            Calendar c = incrementCalendar(queryPeriod, date, sdf);
            date = sdf.format(c.getTime());
            Calendar lastDateCalendar = Calendar.getInstance();
            lastDateCalendar.setTime(sdf.parse(lastDate));

            bindFields(objectNode, timestamp, date, c, lastDateCalendar);
            state = prepareState(query, lowerLongitude, upperLongitude, lowerLatitude, upperLatitude, conn, date, start);
            resultSet = state.executeQuery();
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
            pointCluster.load(points);
            ArrayList<Cluster> clusters = pointCluster.getClusters(new double[]{-180, -90, 180, 90}, 17);
            System.out.println(clusters.size());
            resultSet.close();
            state.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private PreparedStatement prepareState(String query, double lowerLongitude, double upperLongitude, double lowerLatitude, double upperLatitude, Connection conn, String date, String start) throws SQLException {
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
        String timestamp = jsonNode.get("timestamp").asText();
        int zoom = Integer.parseInt(jsonNode.get("zoom").asText());
        String json = "";
        if (pointCluster != null) {
            objectMapper = new ObjectMapper();
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
        ObjectNode objectNode = objectMapper.createObjectNode();
        objectNode.put("option", 1);
        objectNode.put("data", json);
        objectNode.put("timestamp", timestamp);
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
        String json = "";
        objectMapper = new ObjectMapper();
        ObjectNode objectNode = objectMapper.createObjectNode();
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
            System.out.printf("The number of edges is %d\n", edges.size());
            if (bundling == 0) {
                for (Map.Entry<Edge, Integer> entry : edges.entrySet()) {
                    objectNode = objectMapper.createObjectNode();
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
        objectNode.put("option", 2);
        objectNode.put("data", json);
        objectNode.put("timestamp", timestamp);
        System.out.println("json is: " + objectNode.toString());
        bundleActor.returnData(objectNode.toString());
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
