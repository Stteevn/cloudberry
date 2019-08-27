package utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DatabaseUtils {
    private static Connection conn;

    public static Connection getConnection() throws ClassNotFoundException, SQLException {
        if(conn == null) {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/graphtweet", "graphuser",
                    "graphuser");
        }
        return conn;
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
}
