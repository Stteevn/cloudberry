package Utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DatabaseUtils {
    static Connection conn;

    public static Connection getConnection() throws ClassNotFoundException, SQLException {
        if(conn == null) {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/graphtweet", "graphuser",
                    "graphuser");
        }
        return conn;
    }

}
