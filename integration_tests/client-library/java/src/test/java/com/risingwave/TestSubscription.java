package com.risingwave;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TestSubscription {
    public void createAndTest() throws SQLException {
        TestCreateTable.createSourceTable();
        try (Connection conn = TestUtils.establishConnection()) {
            // Step 1: Create a subscription
            String createSubscription = "CREATE SUBSCRIPTION sub4 FROM s1_java WITH (retention = '1 hour')";
            try (PreparedStatement createStmt = conn.prepareStatement(createSubscription)) {
                createStmt.execute();
                System.out.println("Subscription created successfully.");
            }
            // Step 2: Declare a subscription cursor
            String declareCursor = "DECLARE cur4 SUBSCRIPTION CURSOR FOR sub4";
            try (PreparedStatement declareStmt = conn.prepareStatement(declareCursor)) {
                declareStmt.execute();
                System.out.println("Subscription cursor declared successfully.");
            }
            // Step 3: Fetch data from the subscription cursor
            String fetchData = "FETCH NEXT FROM cur4";
            try (PreparedStatement fetchStmt = conn.prepareStatement(fetchData)) {
                ResultSet rs = fetchStmt.executeQuery();
                while (rs.next()) {
                    Object v1 = rs.getObject("v1");
                    assertNotNull(v1);
                }
            }
        }
    }

    public void dropSubscription() throws SQLException {
        try (Connection conn = TestUtils.establishConnection()) {
            PreparedStatement stmt = conn.prepareStatement("DROP SUBSCRIPTION sub4");
            stmt.execute();
            System.out.println("Subscription dropped successfully.");
        }
    }

    @Test
    public void testSubscription() throws SQLException {
        try {
            createAndTest();
        } finally {
            dropSubscription();
            TestCreateTable.dropSourceTable();
        }
    }
}