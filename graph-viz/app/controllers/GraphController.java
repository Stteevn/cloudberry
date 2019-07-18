package controllers;

import actors.BundleActor;
import actors.EchoActor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import play.libs.Json;
import play.mvc.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
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

    static ThreadLocal<ArrayList<Vector>> alldataNodes;
    static ThreadLocal<ArrayList<EdgeVector>> alldataEdges;
    static ThreadLocal<ArrayList<Integer>> allcloseEdgeList;
    static ThreadLocal<Hashtable<Integer, Edge>> edges;
    private File configFile = new File("./conf/config.properties");
    private Properties configProps;

    public void index(String query, EchoActor echoActor) {
        String[] messageAssembler = query.split(" ");
        String endDate = "";
        query = messageAssembler[0];
        double lowerLongitude = Double.parseDouble(messageAssembler[1]);
        double upperLongitude = Double.parseDouble(messageAssembler[2]);
        double lowerLatitude = Double.parseDouble(messageAssembler[3]);
        double upperLatitude = Double.parseDouble(messageAssembler[4]);
        if (messageAssembler.length == 6) {
            endDate = messageAssembler[5];
        }
        else {
            alldataNodes = ThreadLocal.withInitial(ArrayList::new);
            alldataEdges = ThreadLocal.withInitial(ArrayList::new);
            allcloseEdgeList = ThreadLocal.withInitial(ArrayList::new);
            edges = ThreadLocal.withInitial(Hashtable::new);
        }
        String firstDate = null;
        String lastDate = null;
        int queryPeriod = 0;
        String json = "";
        String dt_ret = "";
        long startIndex = System.currentTimeMillis();

        try {
            loadProperties();
            firstDate = configProps.getProperty("firstDate");
            lastDate = configProps.getProperty("lastDate");
            queryPeriod = Integer.parseInt(configProps.getProperty("queryPeriod"));
        } catch (IOException ex) {
            System.out.println("The config.properties file does not exist, default properties loaded.");
        }

        try {
            Class.forName("org.postgresql.Driver");
            Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/graphtweet", "graphuser",
                    "graphuser");

            // compute the start date and end date of each sub_query
            String dt;
            if (endDate.equals("")) {
                dt = firstDate;  // Start date
            } else {
                dt = endDate;
            }

//            System.out.println(dt);
            String start = dt;
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
            Calendar c = Calendar.getInstance();
            c.setTime(sdf.parse(dt));
            c.add(Calendar.HOUR, queryPeriod);  // number of days to add
            dt = sdf.format(c.getTime());  // dt is now the new date
            String end = dt;

            // compute the return string as: Y/N + end date of each sub_query
            String str_stop = lastDate;
            Calendar cal_stop = Calendar.getInstance();
            cal_stop.setTime(sdf.parse(str_stop));

            String searchQuery = "select " + "from_longitude, from_latitude, " + "to_longitude, to_latitude "
                    + "from replytweets where (" + "to_tsvector('english', from_text) " + "@@ to_tsquery( ? ) or "
                    + "to_tsvector('english', to_text) "
                    + "@@ to_tsquery( ? )) and ((from_longitude between ? AND ? AND from_latitude between ? AND ?) OR"
                    + " (to_longitude between ? AND ? AND to_latitude between ? AND ?)) "
                    + "AND to_create_at::timestamp > TO_TIMESTAMP('" + start + "', 'yyyymmddhh24miss') "
                    + "AND to_create_at::timestamp <= TO_TIMESTAMP('" + end + "', 'yyyymmddhh24miss');";

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

            if (!c.before(cal_stop)) {
//                System.out.println("Y" + dt);
                dt_ret = "Y" + dt;
            } else {
//                System.out.println("N" + dt);
                dt_ret = "N" + dt;
            }

            resultSet.close();
            state.close();
            conn.close();
//            System.out.println(json);
//            System.out.println("Total time in parsing data from Json is " + (endIndex - startParse) + " ms");
//            System.out.println("Total time in executing query in database is " + (startParse - startIndex) + " ms");
//            System.out.println("Total time in running index() is " + (endIndex - startIndex) + " ms");
//            System.out.println("Total number of records " + edges.size());
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        echoActor.returnData(dt_ret + json);
    }

    public void reply(String query, BundleActor bundleActor) {
        String[] messageAssembler = query.split(" ");
        String endDate = "";
        query = messageAssembler[0];
        double lowerLongitude = Double.parseDouble(messageAssembler[1]);
        double upperLongitude = Double.parseDouble(messageAssembler[2]);
        double lowerLatitude = Double.parseDouble(messageAssembler[3]);
        double upperLatitude = Double.parseDouble(messageAssembler[4]);
        if (messageAssembler.length == 6) {
            endDate = messageAssembler[5];
        }
        else {
            alldataNodes = ThreadLocal.withInitial(ArrayList::new);
            alldataEdges = ThreadLocal.withInitial(ArrayList::new);
            allcloseEdgeList = ThreadLocal.withInitial(ArrayList::new);
            edges = ThreadLocal.withInitial(Hashtable::new);
        }
        String json = "";
        long startReply = System.currentTimeMillis();
        String dt_ret = "";
        try {
            Edge.set_epsilon(9);
            dt_ret = queryResult(query, endDate, lowerLongitude, upperLongitude, lowerLatitude, upperLatitude);
            long startBundling = System.currentTimeMillis();
//            ArrayList<Vector> dataNodes = new ArrayList<>();
//            ArrayList<EdgeVector> dataEdges = new ArrayList<>();
//            ArrayList<Integer> closeEdgeList = new ArrayList<>();

            for (Map.Entry<Integer, Edge> entry : edges.get().entrySet()) {
                allcloseEdgeList.get().add(entry.getValue().getWeight());
                alldataNodes.get().add(new Vector(entry.getValue().getFromLongitude(), entry.getValue().getFromLatitude()));
                alldataNodes.get().add(new Vector(entry.getValue().getToLongitude(), entry.getValue().getToLatitude()));
                alldataEdges.get().add(new EdgeVector(alldataNodes.get().size() - 2, alldataNodes.get().size() - 1));
            }
            ForceBundling forceBundling = new ForceBundling(alldataNodes.get(), alldataEdges.get());
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
                    lineNode.put("width", allcloseEdgeList.get().get(edgeNum));
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
            System.out.println("Total number of records " + edges.get().size());
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        bundleActor.returnData(dt_ret + json);
    }

    private String queryResult(String query, String endDate, double lowerLongitude, double upperLongitude, double lowerLatitude,
                                               double upperLatitude) {
        Connection conn;
        PreparedStatement state;
        ResultSet resultSet;

        String firstDate = null;
        String lastDate = null;
        int queryPeriod = 0;
        String dt_ret = "";

        try {
            loadProperties();
            firstDate = configProps.getProperty("firstDate");
            lastDate = configProps.getProperty("lastDate");
            queryPeriod = Integer.parseInt(configProps.getProperty("queryPeriod"));
        } catch (IOException ex) {
            System.out.println("The config.properties file does not exist, default properties loaded.");
        }

        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/graphtweet", "graphuser",
                    "graphuser");

            // compute the start date and end date of each sub_query
            String dt;
            if (endDate.equals("")) {
                dt = firstDate;  // Start date
            } else {
                dt = endDate;
            }

            System.out.println(dt);
            String start = dt;
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
            Calendar c = Calendar.getInstance();
            c.setTime(sdf.parse(dt));
            c.add(Calendar.HOUR, queryPeriod);  // number of days to add
            dt = sdf.format(c.getTime());  // dt is now the new date
            String end = dt;

            // compute the return string as: Y/N + end date of each sub_query
            String str_stop = lastDate;
            Calendar cal_stop = Calendar.getInstance();
            cal_stop.setTime(sdf.parse(str_stop));

            String searchQuery = "select " + "from_longitude, from_latitude, " + "to_longitude, to_latitude "
                    + "from replytweets where (" + "to_tsvector('english', from_text) " + "@@ to_tsquery( ? ) or "
                    + "to_tsvector('english', to_text) "
                    + "@@ to_tsquery( ? )) and ((from_longitude between ? AND ? AND from_latitude between ? AND ?) OR"
                    + " (to_longitude between ? AND ? AND to_latitude between ? AND ?)) AND sqrt(pow(from_latitude - to_latitude, 2) + pow(from_longitude - to_longitude, 2)) >= ? "
                    + "AND to_create_at::timestamp > TO_TIMESTAMP('" + start + "', 'yyyymmddhh24miss') "
                    + "AND to_create_at::timestamp <= TO_TIMESTAMP('" + end + "', 'yyyymmddhh24miss');";
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
                if (edges.get().containsKey(currentEdge.getBlock())) {
                    Edge oldEdge = edges.get().get(currentEdge.getBlock());
                    edges.get().remove(currentEdge.getBlock());
                    edges.get().put(currentEdge.getBlock(), oldEdge.updateWeight(1));
                } else {
                    edges.get().put(currentEdge.getBlock(), currentEdge);
                }
            }

            if (!c.before(cal_stop)) {
                System.out.println("Y" + dt);
                dt_ret = "Y" + dt;
            } else {
                System.out.println("N" + dt);
                dt_ret = "N" + dt;
            }

            resultSet.close();
            state.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return dt_ret;
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
