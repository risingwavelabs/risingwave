package com.risingwave.pgwire;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import java.sql.*;
import java.util.Properties;

public class PgwireTest {
    /**
     * You can find a more detailed documentation here:
     * <a>https://jdbc.postgresql.org/documentation/use/</a>
     */
    @Test
    public void testSimpleJdbcQuery() throws Exception {
        String url = "jdbc:postgresql://localhost:4566/dev";
        Properties props = new Properties();
        props.setProperty("user", "root");
        props.setProperty("password", "secret");
        props.setProperty("ssl", "false");
        Connection conn = DriverManager.getConnection(url, props);

        PreparedStatement st = conn.prepareStatement("SELECT CAST(? AS INTEGER) AS number");
        st.setString(1, "2");
        ResultSet rs = st.executeQuery();
        int count = 0;
        while (rs.next()) {
            assertEquals(rs.getInt(1), 2);
            count++;
        }
        assertEquals(count, 1);
        rs.close();
        st.close();
    }
}
