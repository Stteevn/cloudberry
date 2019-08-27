package algorithms;

import actors.BundleActor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import controllers.GraphController;
import utils.DatabaseUtils;
import utils.PropertiesUtil;
import utils.QueryStatement;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;

public class IncrementalQuery {

    private GraphController context;
    // Configuration properties
    private Properties configProps;

    // Indicates the sending process is completed
    private static final String finished = "Y";
    // Indicates the sending process is not completed
    private static final String unfinished = "N";

    public void incrementalQuery(GraphController graphController, String query, BundleActor bundleActor) {
        context = graphController;
        // parse the json and initialize the variables
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode;
        int clusteringAlgo;
        String timestamp;
        String endDate = null;
        String json = null;
        try {
            jsonNode = objectMapper.readTree(query);
            clusteringAlgo = Integer.parseInt(jsonNode.get("clusteringAlgo").asText());
            timestamp = jsonNode.get("timestamp").asText();
            query = jsonNode.get("query").asText();
            if (jsonNode.has("date")) {
                endDate = jsonNode.get("date").asText();
            }
            ObjectNode objectNode = objectMapper.createObjectNode();
            readProperties(query, endDate, clusteringAlgo, timestamp, objectNode);
            objectNode.put("option", 0);
            json = objectNode.toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
        bundleActor.returnData(json);
    }

    private void readProperties(String query, String endDate, int clusteringAlgo, String timestamp, ObjectNode objectNode) {
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
        queryClusterWithSlice(query, endDate, clusteringAlgo, timestamp, objectNode, firstDate, lastDate, queryPeriod);
    }

    private void queryClusterWithSlice(String query, String endDate, int clusteringAlgo, String timestamp, ObjectNode objectNode, String firstDate, String lastDate, int queryPeriod) {
        Connection conn;
        PreparedStatement state;
        ResultSet resultSet;
        Calendar c, lastDateCalendar;
        SimpleDateFormat sdf;
        String start, date;
        try {
            sdf = new SimpleDateFormat("yyyyMMddHHmmss");
            conn = DatabaseUtils.getConnection();
            date = getDate(endDate, firstDate);
            start = date;
            c = incrementCalendar(queryPeriod, date, sdf);
            date = sdf.format(c.getTime());
            lastDateCalendar = Calendar.getInstance();
            lastDateCalendar.setTime(sdf.parse(lastDate));
            bindFields(objectNode, timestamp, date, c, lastDateCalendar);
            state = prepareStatement(query, conn, date, start);
            resultSet = state.executeQuery();
            context.runCluster(query, clusteringAlgo, timestamp, objectNode, firstDate, lastDate, conn, state, resultSet, c, lastDateCalendar, sdf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static PreparedStatement prepareStatement(String query, Connection conn, String date, String start) throws SQLException {
        PreparedStatement state;
        String searchQuery = QueryStatement.incrementalStatement;
        state = conn.prepareStatement(searchQuery);
        state.setString(1, query);
        state.setString(2, query);
        state.setString(3, start);
        state.setString(4, date);
        return state;
    }

    public static void bindFields(ObjectNode objectNode, String timestamp, String date, Calendar c, Calendar lastDateCalendar) {
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
}
