package org.openjdbcproxy.grpc.server;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

class CircuitBreakerTest {

    @Test
    void testAllowsWhenNoFailures() {
        CircuitBreaker breaker = new CircuitBreaker(1000);
        assertDoesNotThrow(() -> breaker.preCheck("SELECT 1"));
    }

    @Test
    void testBlocksAfterThreeFailures() {
        CircuitBreaker breaker = new CircuitBreaker(5000);
        String sql = "SELECT * FROM test";
        SQLException ex = new SQLException("fail");
        // Fail three times
        breaker.onFailure(sql, ex);
        breaker.onFailure(sql, ex);
        breaker.onFailure(sql, ex);

        SQLException thrown = assertThrows(SQLException.class, () -> breaker.preCheck(sql));
        assertEquals("fail", thrown.getMessage());
    }

    @Test
    void testAllowsAgainAfterOpenTimeoutAndSuccessResets() throws InterruptedException, SQLException {
        CircuitBreaker breaker = new CircuitBreaker(300);
        String sql = "UPDATE X SET Y=1";
        SQLException ex = new SQLException("fail");

        // Trip breaker
        breaker.onFailure(sql, ex);
        breaker.onFailure(sql, ex);
        breaker.onFailure(sql, ex);
        assertThrows(SQLException.class, () -> breaker.preCheck(sql));

        // Wait for open period to pass
        Thread.sleep(400);
        // Should allow one through (half-open)
        assertDoesNotThrow(() -> breaker.preCheck(sql));
        // Success should reset
        breaker.onSuccess(sql);
        assertDoesNotThrow(() -> breaker.preCheck(sql));
    }

    @Test
    void testResetsOnSuccess() throws SQLException {
        CircuitBreaker breaker = new CircuitBreaker(1000);
        String sql = "INSERT X";
        SQLException ex = new SQLException("fail2");
        breaker.onFailure(sql, ex);
        breaker.onFailure(sql, ex);
        breaker.onFailure(sql, ex);
        assertThrows(SQLException.class, () -> breaker.preCheck(sql));
        breaker.onSuccess(sql);
        assertDoesNotThrow(() -> breaker.preCheck(sql));
    }

    @Test
    void testOnFailureIsNoOpWhenAlreadyOpen() {
        CircuitBreaker breaker = new CircuitBreaker(500);
        String sql = "SELECT fail";
        SQLException ex1 = new SQLException("fail1");
        SQLException ex2 = new SQLException("fail2");
        // Trip breaker
        breaker.onFailure(sql, ex1);
        breaker.onFailure(sql, ex1);
        breaker.onFailure(sql, ex1);

        // Now breaker is open, further failures should not change lastError
        breaker.onFailure(sql, ex2);

        SQLException thrown = assertThrows(SQLException.class, () -> breaker.preCheck(sql));
        assertEquals("fail1", thrown.getMessage());
    }
}