package algorithms;

import actors.BundleActor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import controllers.GraphController;
import utils.PropertiesUtil;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;

public class IncrementalQuery {

    private GraphController context;
    // Configuration properties
    private Properties configProps;

    public void prepareIncremental(GraphController graphController, String query, BundleActor bundleActor) {
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
        Calendar c, lastDateCalendar;
        SimpleDateFormat sdf;
        String start, date;
        try {
            sdf = new SimpleDateFormat("yyyyMMddHHmmss");
            date = getDate(endDate, firstDate);
            start = date;
            c = incrementCalendar(queryPeriod, date, sdf);
            date = sdf.format(c.getTime());
            lastDateCalendar = Calendar.getInstance();
            lastDateCalendar.setTime(sdf.parse(lastDate));
            context.doIncrementalQuery(query, clusteringAlgo, timestamp, objectNode, firstDate, lastDate, c, lastDateCalendar, sdf, start, date);
        } catch (Exception e) {
            e.printStackTrace();
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
