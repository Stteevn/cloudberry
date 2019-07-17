package controllers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import play.mvc.*;

import java.sql.*;
import java.util.*;

/**
 * This controller contains an action to handle HTTP requests to the
 * application's home page.
 */

public class GraphController extends Controller {

    /**
     * An action that renders an HTML page with a welcome message. The configuration
     * in the <code>routes</code> file means that this method will be called when
     * the application receives a <code>GET</code> request with a path of
     * <code>/</code>.
     */

    public Result index(String query, double lowerLongitude, double upperLongitude, double lowerLatitude,
                        double upperLatitude) {
        String json = "";
        long startIndex = System.currentTimeMillis();
        try {
            Class.forName("org.postgresql.Driver");
            Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/graphtweet", "graphuser",
                    "graphuser");
            String searchQuery = "select " + "from_longitude, from_latitude, " + "to_longitude, to_latitude "
                    + "from replytweets where (" + "to_tsvector('english', from_text) " + "@@ to_tsquery( ? ) or "
                    + "to_tsvector('english', to_text) "
                    + "@@ to_tsquery( ? )) and ((from_longitude between ? AND ? AND from_latitude between ? AND ?) OR"
                    + " (to_longitude between ? AND ? AND to_latitude between ? AND ?));";

            PreparedStatement state = conn.prepareStatement(searchQuery);
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
            ResultSet resultSet = state.executeQuery();
            Set<Edge> edges = new HashSet<>();
            long startParse = System.currentTimeMillis();
            while (resultSet.next()) {
                double fromLongtitude = resultSet.getDouble("from_longitude");
                double fromLatitude = resultSet.getDouble("from_latitude");
                double toLongtitude = resultSet.getDouble("to_longitude");
                double toLatitude = resultSet.getDouble("to_latitude");
                Edge currentEdge = new Edge(fromLatitude, fromLongtitude, toLatitude, toLongtitude);
                edges.add(currentEdge);
            }
            ObjectMapper objectMapper = new ObjectMapper();
            SimpleModule module = new SimpleModule();
            module.addSerializer(Edge.class, new EdgeSerializer());
            objectMapper.registerModule(module);
            json = objectMapper.writeValueAsString(edges);
            long endIndex = System.currentTimeMillis();
            resultSet.close();
            state.close();
            conn.close();
            System.out.println(json);
            System.out.println("Total time in parsing data from Json is " + (endIndex - startParse) + " ms");
            System.out.println("Total time in executing query in database is " + (startParse - startIndex) + " ms");
            System.out.println("Total time in running index() is " + (endIndex - startIndex) + " ms");
            System.out.println("Total number of records " + edges.size());
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return ok(json);
    }

    public Result reply(String query, double lowerLongitude, double upperLongitude, double lowerLatitude,
                        double upperLatitude) {
        String json = "";
        long startReply = System.currentTimeMillis();
        try {
            Edge.set_epsilon(9);
            HashMap<Integer, Edge> edges = queryResult(query, lowerLongitude, upperLongitude, lowerLatitude, upperLatitude);

            long startBundling = System.currentTimeMillis();
            ArrayList<Vector> dataNodes = new ArrayList<>();
            ArrayList<EdgeVector> dataEdges = new ArrayList<>();
            ArrayList<Integer> closeEdgeList = new ArrayList<>();

            for (Map.Entry<Integer, Edge> entry : edges.entrySet()) {
                closeEdgeList.add(entry.getValue().getWeight());
                dataNodes.add(new Vector(entry.getValue().getFromLongitude(), entry.getValue().getFromLatitude()));
                dataNodes.add(new Vector(entry.getValue().getToLongitude(), entry.getValue().getToLatitude()));
                dataEdges.add(new EdgeVector(dataNodes.size() - 2, dataNodes.size() - 1));
            }
            ForceBundling forceBundling = new ForceBundling(dataNodes, dataEdges);
            ArrayList<Path> pathResult = forceBundling.forceBundle();
            ObjectMapper objectMapper = new ObjectMapper();
            ArrayNode pathJson = objectMapper.createArrayNode();
            long startParse = System.currentTimeMillis();
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
            long endReply = System.currentTimeMillis();
            System.out.println("Total time in parsing data from Json is " + (endReply - startParse) + " ms");
            System.out.println("Total time in executing query in database is " + (startBundling - startReply) + " ms");
            System.out.println("Total time in edge bundling is " + (startParse - startBundling) + " ms");
            System.out.println("Total time in running replay() is " + (endReply - startReply) + " ms");
            System.out.println("Total number of records " + edges.size());
        } catch (Exception e) {
            System.out.println(e.getMessage());
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
}
