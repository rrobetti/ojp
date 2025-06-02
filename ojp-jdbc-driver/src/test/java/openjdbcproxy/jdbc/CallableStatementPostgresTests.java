package openjdbcproxy.jdbc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Disabled
public class CallableStatementPostgresTests {

    private Connection connection;
    private CallableStatement callableStatement;

    @BeforeEach
    public void setUp() throws Exception {
        // Connect to the PostgreSQL database
        connection = DriverManager.getConnection(
                "jdbc:postgresql://db-postgresql-lon1-29621-do-user-2684437-0.l.db.ondigitalocean.com:25060/defaultdb?sslmode=require",
                "doadmin",
                "AVNS_YDEqaTP11rsMTEDLV7O"
        );

        // Ensure the employee table and stored procedure exist
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS employee");
            stmt.execute("CREATE TABLE employee (id SERIAL PRIMARY KEY, name VARCHAR(255), salary NUMERIC(10, 2))");
            stmt.execute("INSERT INTO employee (name, salary) VALUES ('Alice', 50000)");

            stmt.execute(
                    "CREATE OR REPLACE PROCEDURE update_salary(" +
                            "    emp_id INT," +
                            "    new_salary NUMERIC(10,2)," +
                            "    OUT updated_salary NUMERIC(10,2)" +
                            ") LANGUAGE plpgsql AS $$ " +
                            "BEGIN " +
                            "    UPDATE employee SET salary = new_salary WHERE id = emp_id;" +
                            "    SELECT salary INTO updated_salary FROM employee WHERE id = emp_id;" +
                            "END; $$;"
            );
        }
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (callableStatement != null) callableStatement.close();
        if (connection != null) connection.close();
    }

    @Test
    public void testExecuteProcedure() throws Exception {
        // Prepare the callable statement for the stored procedure
        callableStatement = connection.prepareCall("CALL update_salary(?, ?, ?)");
        callableStatement.setInt(1, 1); // Set the first parameter (employee ID)
        callableStatement.setBigDecimal(2, new BigDecimal("60000")); // Set the second parameter (new salary)
        callableStatement.registerOutParameter(3, Types.NUMERIC); // Register the output parameter (updated salary)

        // Execute the stored procedure
        callableStatement.execute();

        // Retrieve the output parameter
        BigDecimal updatedSalary = callableStatement.getBigDecimal(3);
        assertNotNull(updatedSalary, "The updated salary should not be null.");
        assertEquals(new BigDecimal("60000.00"), updatedSalary);

        // Verify the salary update in the database
        ResultSet resultSet = connection
                .createStatement()
                .executeQuery("SELECT salary FROM employee WHERE id = 1");
        assertTrue(resultSet.next());
        assertEquals(new BigDecimal("60000.00"), resultSet.getBigDecimal("salary"));
    }

    @Test
    public void testInvalidParameterIndex() {
        // This test will intentionally fail due to an invalid parameter index
        assertThrows(SQLException.class, () -> {
            callableStatement = connection.prepareCall("{ CALL update_salary(?, ?, ?) }");
            callableStatement.setInt(4, 1); // Invalid parameter index (should be 1, 2, or 3)
            callableStatement.execute();
        });
    }
}