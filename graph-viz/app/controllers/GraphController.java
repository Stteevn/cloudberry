package controllers;

import actors.BundleActor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import play.mvc.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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

    private static ArrayList<Vector> alldataNodes;
    private static ArrayList<EdgeVector> alldataEdges;
    private static ArrayList<Integer> weights;
    private static Hashtable<Integer, Edge> edges;
    // configuration file
    private File configFile = new File("./conf/config.properties");
    private Properties configProps;
    // indicates the sending process is completed
    private static final String finished = "Y";
    // indicates the sending process is not completed
    private static final String unfinished = "N";

    public void bundle(String query, BundleActor bundleActor) {
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
        query = jsonNode.get("query").asText();
        String endDate = null;
        if (jsonNode.has("date")) {
            endDate = jsonNode.get("date").asText();
        }else{
            alldataNodes = new ArrayList<>();
            alldataEdges = new ArrayList<>();
            weights = new ArrayList<>();
            edges = new Hashtable<>();
        }
        String json = "";
        long startReply = System.currentTimeMillis();
        ObjectNode objectNode = objectMapper.createObjectNode();
        try {
            Edge.set_epsilon(9);
            objectNode = queryResult(query, endDate, lowerLongitude, upperLongitude, lowerLatitude, upperLatitude, objectNode);
            long startBundling = System.currentTimeMillis();
            for (Map.Entry<Integer, Edge> entry : edges.entrySet()) {
                weights.add(entry.getValue().getWeight());
                alldataNodes.add(new Vector(entry.getValue().getFromLongitude(), entry.getValue().getFromLatitude()));
                alldataNodes.add(new Vector(entry.getValue().getToLongitude(), entry.getValue().getToLatitude()));
                alldataEdges.add(new EdgeVector(alldataNodes.size() - 2, alldataNodes.size() - 1));
            }
            ForceBundling forceBundling = new ForceBundling(alldataNodes, alldataEdges);
            ArrayList<Path> pathResult = forceBundling.forceBundle();
            objectMapper = new ObjectMapper();
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
                    lineNode.put("width", weights.get(edgeNum));
                    pathJson.add(lineNode);
                }
                edgeNum++;
            }
            objectNode.set("data", pathJson);
            json = objectNode.toString();
            long endReply = System.currentTimeMillis();
            System.out.println("Total time in parsing data from Json is " + (endReply - startParse) + " ms");
            System.out.println("Total time in executing query in database is " + (startBundling - startReply) + " ms");
            System.out.println("Total time in edge bundling is " + (startParse - startBundling) + " ms");
            System.out.println("Total time in running replay() is " + (endReply - startReply) + " ms");
            System.out.println("Total number of records " + edges.size());
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        bundleActor.returnData(json);

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

    private ObjectNode queryResult(String query, String endDate, double lowerLongitude, double upperLongitude, double lowerLatitude,
                                               double upperLatitude, ObjectNode objectNode) {
        String firstDate = null;
        String lastDate = null;
        int queryPeriod = 0;

        try {
            loadProperties();
            firstDate = configProps.getProperty("firstDate");
            lastDate = configProps.getProperty("lastDate");
            queryPeriod = Integer.parseInt(configProps.getProperty("queryPeriod"));
        } catch (IOException ex) {
            System.out.println("The config.properties file does not exist, default properties loaded.");
        }

        getData(query, endDate, lowerLongitude, upperLongitude, lowerLatitude, upperLatitude, objectNode, firstDate, lastDate, queryPeriod);
        return objectNode;
    }

    private void getData(String query, String endDate, double lowerLongitude, double upperLongitude, double lowerLatitude, double upperLatitude, ObjectNode objectNode, String firstDate, String lastDate, int queryPeriod) {
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

            if (!c.before(lastDateCalendar)) {
                System.out.println(finished + date);
                objectNode.put("flag", finished);
                objectNode.put("date", date);
            } else {
                System.out.println(unfinished + date);
                objectNode.put("flag", unfinished);
                objectNode.put("date", date);
            }
            state = prepareState(query, lowerLongitude, upperLongitude, lowerLatitude, upperLatitude, conn, date, start);
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
    }

    private PreparedStatement prepareState(String query, double lowerLongitude, double upperLongitude, double lowerLatitude, double upperLatitude, Connection conn, String date, String start) throws SQLException {
        PreparedStatement state;
        String searchQuery = "select from_longitude, from_latitude, to_longitude, to_latitude "
                + "from replytweets where ( to_tsvector('english', from_text) @@ to_tsquery( ? ) or "
                + "to_tsvector('english', to_text) "
                + "@@ to_tsquery( ? )) and ((from_longitude between ? AND ? AND from_latitude between ? AND ?) OR"
                + " (to_longitude between ? AND ? AND to_latitude between ? AND ?)) AND sqrt(pow(from_latitude - to_latitude, 2) + pow(from_longitude - to_longitude, 2)) >= ? "
                + "AND to_create_at::timestamp > TO_TIMESTAMP( ? , 'yyyymmddhh24miss') "
                + "AND to_create_at::timestamp <= TO_TIMESTAMP( ? , 'yyyymmddhh24miss');";
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
        state.setString(12, start);
        state.setString(13, date);
        return state;
    }

    private void loadProperties() throws IOException {
        Properties defaultProps = new Properties();
        // sets default properties
        defaultProps.setProperty("firstDate", "20180101");
        defaultProps.setProperty("lastDate", "20181231");
        defaultProps.setProperty("queryPeriod", "10");

        configProps = new Properties(defaultProps);

        // loads properties from file
        InputStream inputStream = new FileInputStream(configFile);
        configProps.load(inputStream);
        inputStream.close();
    }
}
