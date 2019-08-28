package algorithms;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import controllers.GraphController;
import utils.PropertiesUtil;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;

/**
 * Implement the incremental database query
 */
public class IncrementalQuery {

    // Saved graphController context for returning back
    private GraphController context;
    // Configuration properties
    private Properties configProps;

    /**
     * Prepare incremental, read the relative parameters from the request
     * @param graphController reference graphController context
     * @param query request message string
     */
    public void prepareIncremental(GraphController graphController, String query) {
        context = graphController;
        // parse the json and initialize the variables
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode;
        String endDate = null;
        try {
            jsonNode = objectMapper.readTree(query);
            if (jsonNode.has("date")) {
                endDate = jsonNode.get("date").asText();
            }
            readProperties(query, endDate);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Reads the properties file.
     * @param query query message string
     * @param endDate endDate parsed from query string
     */
    private void readProperties(String query, String endDate) {
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
        calculateDate(query, endDate, firstDate, lastDate, queryPeriod);
    }

    /**
     * Calculates the target query dates.
     * @param query query message string
     * @param endDate endDate parsed from query string
     * @param firstDate first date in the database read from configuration file
     * @param lastDate last date in the database read from configuration file
     * @param queryPeriod query period in the database read from configuration file
     */
    private void calculateDate(String query, String endDate, String firstDate, String lastDate, int queryPeriod) {
        Calendar c, lastDateCalendar;
        SimpleDateFormat sdf;
        String start, date;
        try {
            sdf = new SimpleDateFormat("yyyyMMddHHmmss");
            date = (endDate == null) ? firstDate : endDate;
            start = date;
            c = incrementCalendar(queryPeriod, date, sdf);
            date = sdf.format(c.getTime());
            lastDateCalendar = Calendar.getInstance();
            lastDateCalendar.setTime(sdf.parse(lastDate));
            context.doIncrementalQuery(query, firstDate, lastDate, c, lastDateCalendar, sdf, start, date);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Incremental the date by query period。
     * @param queryPeriod query period in the database read from configuration file
     * @param date base date
     * @param sdf date format
     * @return Calendar object after incremental
     */
    private Calendar incrementCalendar(int queryPeriod, String date, SimpleDateFormat sdf) {
        Calendar c = Calendar.getInstance();
        try {
            c.setTime(sdf.parse(date));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        c.add(Calendar.HOUR, queryPeriod);  // number of days to add
        return c;
    }

}
