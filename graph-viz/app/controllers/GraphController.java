package controllers;

import com.fasterxml.jackson.databind.ObjectMapper;
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
            System.out.println(state.toString());
            ResultSet resultSet = state.executeQuery();
            HashMap<Edge, Integer> edges = new HashMap<>();

            Edge.set_epslion(0);
            long startParse = System.currentTimeMillis();
            while (resultSet.next()) {
                double fromLongitude = resultSet.getDouble("from_longitude");
                double fromLatitude = resultSet.getDouble("from_latitude");
                double toLongitude = resultSet.getDouble("to_longitude");
                double toLatitude = resultSet.getFloat("to_latitude");

                Edge currentEdge = new Edge(fromLatitude, fromLongitude, toLatitude, toLongitude);
                if (edges.containsKey(currentEdge)) {
                    edges.put(currentEdge, edges.get(currentEdge) + 1);
                } else {
                    edges.put(currentEdge, 1);
                }
            }

            ObjectMapper objectMapper = new ObjectMapper();

            ArrayNode jsonNodes = objectMapper.createArrayNode();

            for(Map.Entry<Edge, Integer> entry : edges.entrySet()) {
                ObjectNode objectNode = objectMapper.createObjectNode();
                ArrayNode fromNode = objectMapper.createArrayNode();
                fromNode.add(entry.getKey().getFromLongitude());
                fromNode.add(entry.getKey().getFromLatitude());
                ArrayNode toNode = objectMapper.createArrayNode();
                toNode.add(entry.getKey().getToLongitude());
                toNode.add(entry.getKey().getToLatitude());
                objectNode.put("width", entry.getValue());
                objectNode.putArray("source").addAll(fromNode);
                objectNode.putArray("target").addAll(toNode);
                jsonNodes.add(objectNode);
            }

            json = jsonNodes.toString();
            long endIndex = System.currentTimeMillis();
            resultSet.close();
            state.close();
            conn.close();
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
            System.out.println(state.toString());
            ResultSet resultSet = state.executeQuery();
            HashMap<Edge, Integer> edges = new HashMap<>();
            Edge.set_epslion(5);

            long startParse = System.currentTimeMillis();
            while (resultSet.next()) {
                double fromLongitude = resultSet.getDouble("from_longitude");
                double fromLatitude = resultSet.getDouble("from_latitude");
                double toLongitude = resultSet.getDouble("to_longitude");
                double toLatitude = resultSet.getFloat("to_latitude");
                Edge currentEdge = new Edge(fromLatitude, fromLongitude, toLatitude, toLongitude);
                if (edges.containsKey(currentEdge)) {
                    edges.put(currentEdge, edges.get(currentEdge) + 1);
                } else {
                    edges.put(currentEdge, 1);
                }
            }

            ArrayList<Vector> dataNodes = new ArrayList<>();
            ArrayList<EdgeVector> dataEdges = new ArrayList<>();

            ArrayList<Integer> sameEdgeList = new ArrayList<>();
            for (Map.Entry<Edge, Integer> entry : edges.entrySet()) {
                sameEdgeList.add(entry.getValue());
                dataNodes.add(new Vector(entry.getKey().getFromLongitude(), entry.getKey().getFromLatitude()));
                dataNodes.add(new Vector(entry.getKey().getToLongitude(), entry.getKey().getToLatitude()));
                dataEdges.add(new EdgeVector(dataNodes.size() - 2, dataNodes.size() - 1));
            }
            ForceBundling forceBundling = new ForceBundling(dataNodes, dataEdges);
            ArrayList<Path> pathResult = forceBundling.forceBundle();

            ObjectMapper objectMapper = new ObjectMapper();

            ArrayNode pathJson = objectMapper.createArrayNode();

            int edgeNum = 0;
            for(Path path : pathResult) {
                for(int j = 0; j < path.alv.size() - 1; j++) {
                    ObjectNode lineNode = objectMapper.createObjectNode();
                    ArrayNode fromArray = objectMapper.createArrayNode();
                    fromArray.add(path.alv.get(j).x);
                    fromArray.add(path.alv.get(j).y);
                    ArrayNode toArray = objectMapper.createArrayNode();
                    toArray.add(path.alv.get(j + 1).x);
                    toArray.add(path.alv.get(j + 1).y);
                    lineNode.putArray("from").addAll(fromArray);
                    lineNode.putArray("to").addAll(toArray);
                    lineNode.put("width", sameEdgeList.get(edgeNum));
                    pathJson.add(lineNode);
                }
                edgeNum++;
            }
            json = pathJson.toString();
            long endIndex = System.currentTimeMillis();
            resultSet.close();
            state.close();
            conn.close();
            System.out.println("Total time in parsing data from Json is " + (endIndex - startParse) + " ms");
            System.out.println("Total time in executing query in database is " + (startParse - startIndex) + " ms");
            System.out.println("Total time in running index() is " + (endIndex - startIndex) + " ms");
            System.out.println("Total number of records " + edges.size());
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return ok(json);
    }
}
