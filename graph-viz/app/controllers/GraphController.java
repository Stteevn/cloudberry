package controllers;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;

/**
 * This controller contains an action to handle HTTP requests
 * to the application's home page.
 */

public class GraphController extends Controller {

    /**
     * An action that renders an HTML page with a welcome message.
     * The configuration in the <code>routes</code> file means that
     * this method will be called when the application receives a
     * <code>GET</code> request with a path of <code>/</code>.
     */


    public Result index(String query) {
        ArrayNode replies = Json.newArray();
        try {
            Class.forName("org.postgresql.Driver");
            Connection conn = DriverManager
                    .getConnection("jdbc:postgresql://localhost:5432/graphtweet",
                            "graphuser", "graphuser");
            Statement statement = conn.createStatement();

            ResultSet resultSet = statement.executeQuery("select " +
                    "from_longitude, from_latitude, " +
                    "to_longitude, to_latitude " +
                    "from replytweets where " +
                    "to_tsvector('english', from_text) " +
                    "@@ to_tsquery('" + query + "') or " +
                    "to_tsvector('english', to_text) " +
                    "@@ to_tsquery('" + query + "');");

            while (resultSet.next()) {
                ObjectNode reply = Json.newObject();
                ArrayNode fromCoordinate = Json.newArray();
                fromCoordinate.add(resultSet.getFloat("from_longitude"));
                fromCoordinate.add(resultSet.getFloat("from_latitude"));
                reply.set("source", fromCoordinate);
                ArrayNode toCoordinate = Json.newArray();
                toCoordinate.add(resultSet.getFloat("to_longitude"));
                toCoordinate.add(resultSet.getFloat("to_latitude"));
                reply.set("target", toCoordinate);
                replies.add(reply);
            }

            resultSet.close();
            statement.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ok(Json.toJson(replies));
    }

    public Result reply(String query) {
        ArrayNode replies = Json.newArray();
        try {
            Class.forName("org.postgresql.Driver");
            Connection conn = DriverManager
                    .getConnection("jdbc:postgresql://localhost:5432/graphtweet",
                            "graphuser", "graphuser");
            Statement statement = conn.createStatement();

            ResultSet resultSet = statement.executeQuery("select " +
                    "from_longitude, from_latitude, " +
                    "to_longitude, to_latitude " +
                    "from replytweets where " +
                    "to_tsvector('english', from_text) " +
                    "@@ to_tsquery('" + query + "') or " +
                    "to_tsvector('english', to_text) " +
                    "@@ to_tsquery('" + query + "');");
            ArrayList<Vector> dataNodes = new ArrayList<>();
            ArrayList<Edge> dataEdges = new ArrayList<>();
            while (resultSet.next()) {
                Vector source = new Vector(resultSet.getFloat("from_longitude"), resultSet.getFloat("from_latitude"));
                Vector target = new Vector(resultSet.getFloat("to_longitude"), resultSet.getFloat("to_latitude"));
                double eps = 1e-4;
                if (Math.sqrt(Math.pow(source.x - target.x, 2) + Math.pow(source.y - target.y, 2)) <= eps) continue;
                dataNodes.add(source);
                dataNodes.add(target);
                dataEdges.add(new Edge(dataNodes.size() - 2, dataNodes.size() - 1));
            }

            System.out.println("Edge bundling");
            ForceBundling forceBundling = new ForceBundling(dataNodes, dataEdges);
            ArrayList<Path> pathResult = forceBundling.forceBundle();
            System.out.println(pathResult.size());
            for (Path path : pathResult) {
                ObjectNode reply = Json.newObject();
                ArrayNode pathArray = Json.newArray();
                for (Vector vector : path.alv) {
                    ArrayNode vectorNode = Json.newArray();
                    vectorNode.add(vector.x);
                    vectorNode.add(vector.y);
                    pathArray.add(vectorNode);
                }
                reply.set("path", pathArray);
                replies.add(reply);
            }

            resultSet.close();
            statement.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ok(Json.toJson(replies));
    }
}
