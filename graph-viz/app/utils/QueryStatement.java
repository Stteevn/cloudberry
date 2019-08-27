package utils;

public class QueryStatement {
    public static String incrementalStatement = "select from_longitude, from_latitude, to_longitude, to_latitude "
            + "from replytweets where ( to_tsvector('english', from_text) @@ to_tsquery( ? ) or "
            + "to_tsvector('english', to_text) "
            + "@@ to_tsquery( ? )) AND to_create_at::timestamp > TO_TIMESTAMP( ? , 'yyyymmddhh24miss') "
            + "AND to_create_at::timestamp <= TO_TIMESTAMP( ? , 'yyyymmddhh24miss');";
}
