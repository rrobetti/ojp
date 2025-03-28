package org.openjdbcproxy.jdbc;

import com.openjdbcproxy.grpc.OpContext;
import com.openjdbcproxy.grpc.OpResult;
import org.openjdbcproxy.grpc.client.StatementService;
import org.openjdbcproxy.grpc.dto.OpQueryResult;
import org.openjdbcproxy.grpc.dto.Parameter;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.Ref;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.openjdbcproxy.grpc.dto.ParameterType.*;
import static org.openjdbcproxy.grpc.SerializationHandler.deserialize;

public class PreparedStatement implements java.sql.PreparedStatement {
    private final OpContext ctx;
    private String sql;
    private SortedMap<Integer, Parameter> paramsMap;
    private StatementService statementService;

    public PreparedStatement(OpContext ctx, String sql, StatementService statementService) {
        this.ctx = ctx;
        this.sql = sql;
        this.paramsMap = new TreeMap<>();
        this.statementService = statementService;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        OpQueryResult queryResult = this.statementService
                .executeQuery(this.ctx, this.sql, this.paramsMap.values().stream().toList());
        return new ResultSet(queryResult, this.statementService, queryResult.getRows());
    }

    @Override
    public int executeUpdate() throws SQLException {
        return this.statementService.executeUpdate(this.ctx, this.sql, this.paramsMap.values().stream().toList());
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        this.paramsMap.put(parameterIndex,
            Parameter.builder()
                    .type(NULL)
                    .build());
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        this.paramsMap.put(parameterIndex,
            Parameter.builder()
                .type(BOOLEAN)
                .values(List.of(x))
                .build());
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        this.paramsMap.put(parameterIndex,
            Parameter.builder()
                    .type(BYTE)
                    .values(List.of(x))
                    .build());
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        this.paramsMap.put(parameterIndex,
            Parameter.builder()
                    .type(SHORT)
                    .values(List.of(x))
                    .build());
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        this.paramsMap.put(parameterIndex,
            Parameter.builder()
                    .type(INT)
                    .values(List.of(x))
                    .build());
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        this.paramsMap.put(parameterIndex,
            Parameter.builder()
                    .type(LONG)
                    .values(List.of(x))
                    .build());
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        this.paramsMap.put(parameterIndex,
            Parameter.builder()
                    .type(FLOAT)
                    .values(List.of(x))
                    .build());
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        this.paramsMap.put(parameterIndex,
            Parameter.builder()
                    .type(DOUBLE)
                    .values(List.of(x))
                    .build());
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        this.paramsMap.put(parameterIndex,
            Parameter.builder()
                    .type(BIG_DECIMAL)
                    .values(List.of(x))
                    .build());
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        this.paramsMap.put(parameterIndex,
            Parameter.builder()
                    .type(STRING)
                    .values(List.of(x))
                    .build());
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        this.paramsMap.put(parameterIndex,
            Parameter.builder()
                    .type(BYTES)
                    .values(List.of(x))
                    .build());
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        this.paramsMap.put(parameterIndex,
            Parameter.builder()
                    .type(DATE)
                    .values(List.of(x))
                    .build());
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        this.paramsMap.put(parameterIndex,
            Parameter.builder()
                    .type(TIME)
                    .values(List.of(x))
                    .build());
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder().type(TIMESTAMP).values(List.of(x)).build());
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        this.paramsMap.put(parameterIndex,
            Parameter.builder()
                    .type(ASCII_STREAM)
                    .values(List.of(x))
                    .build());
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        this.paramsMap.put(parameterIndex,
            Parameter.builder()
                    .type(UNICODE_STREAM)
                    .values(List.of(x))
                    .build());
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        this.paramsMap.put(parameterIndex,
            Parameter.builder()
                    .type(BINARY_STREAM)
                    .values(List.of(x))
                    .build());
}

    @Override
    public void clearParameters() throws SQLException {
        this.paramsMap = new TreeMap<>();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        this.paramsMap.put(parameterIndex,
            Parameter.builder()
                    .type(OBJECT)
                    .values(List.of(x, targetSqlType))
                    .build());
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        this.paramsMap.put(parameterIndex,
            Parameter.builder()
                    .type(OBJECT)
                    .values(List.of(x))
                    .build());
    }

    @Override
    public boolean execute() throws SQLException {
        return false;
    }

    @Override
    public void addBatch() throws SQLException {

    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        //TODO this will require an implementation of Reader that communicates across GRPC
        this.paramsMap.put(parameterIndex,
            Parameter.builder()
                    .type(CHARACTER_READER)
                    .values(List.of(reader, length))
                    .build());
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        this.paramsMap.put(parameterIndex,
            Parameter.builder()
                    .type(REF)
                    .values(List.of(x))
                    .build());
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        this.paramsMap.put(parameterIndex,
            Parameter.builder()
                    .type(BLOB)
                    .values(List.of(x))
                    .build());
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        this.paramsMap.put(parameterIndex,
            Parameter.builder()
                    .type(CLOB)
                    .values(List.of(x))
                    .build());
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        this.paramsMap.put(parameterIndex,
            Parameter.builder()
                    .type(ARRAY)
                    .values(List.of(x))
                    .build());
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return null;
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        this.paramsMap.put(parameterIndex,
            Parameter.builder()
                    .type(DATE)
                    .values(List.of(x, cal))
                    .build());
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        this.paramsMap.put(parameterIndex,
            Parameter.builder()
                    .type(TIME)
                    .values(List.of(x, cal))
                    .build());
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        this.paramsMap.put(parameterIndex,
            Parameter.builder()
                    .type(TIMESTAMP)
                    .values(List.of(x, cal))
                    .build());
}

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        this.paramsMap.put(parameterIndex,
            Parameter.builder()
                    .type(NULL)
                    .values(List.of(sqlType, typeName))
                    .build());
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        this.paramsMap.put(parameterIndex,
            Parameter.builder()
                    .type(URL)
                    .values(List.of(x))
                    .build());
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return null;
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        this.paramsMap.put(parameterIndex,
            Parameter.builder()
                    .type(ROW_ID)
                    .values(List.of(x))
                    .build());
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        this.paramsMap.put(parameterIndex,
            Parameter.builder()
                    .type(N_STRING)
                    .values(List.of(value))
                    .build());
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        //TODO see if can use similar/same reader communication layer as other methods that require reader
        this.paramsMap.put(parameterIndex,
            Parameter.builder()
                    .type(N_CHARACTER_STREAM)
                    .values(List.of(value, length))
                    .build());
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        this.paramsMap.put(parameterIndex,
            Parameter.builder()
                    .type(N_CLOB)
                    .values(List.of(value))
                    .build());
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        //TODO see if can use similar/same reader communication layer as other methods that require reader
        this.paramsMap.put(parameterIndex,
            Parameter.builder()
                    .type(CLOB)
                    .values(List.of(reader, length))
                    .build());
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        //TODO see if can use similar/same reader communication layer as other methods that require reader
        this.paramsMap.put(parameterIndex,
            Parameter.builder()
                    .type(BLOB)
                    .values(List.of(inputStream, length))
                    .build());
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        //TODO see if can use similar/same reader communication layer as other methods that require reader
        this.paramsMap.put(parameterIndex,
            Parameter.builder()
                    .type(N_CLOB)
                    .values(List.of(reader, length))
                    .build());
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        this.paramsMap.put(parameterIndex,
            Parameter.builder()
                    .type(SQL_XML)
                    .values(List.of(xmlObject))
                    .build());
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        this.paramsMap.put(parameterIndex,
            Parameter.builder()
                    .type(OBJECT)
                    .values(List.of(x, targetSqlType, scaleOrLength))
                    .build());
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        this.paramsMap.put(parameterIndex,
            Parameter.builder()
                    .type(ASCII_STREAM)
                    .values(List.of(x, length))
                    .build());
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        this.paramsMap.put(parameterIndex,
            Parameter.builder()
                    .type(BINARY_STREAM)
                    .values(List.of(x, length))
                    .build());
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {

    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {

    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {

    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {

    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {

    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {

    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        return null;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        return 0;
    }

    @Override
    public void close() throws SQLException {

    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {

    }

    @Override
    public int getMaxRows() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {

    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {

    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return 0;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {

    }

    @Override
    public void cancel() throws SQLException {

    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public void setCursorName(String name) throws SQLException {

    }

    @Override
    public boolean execute(String sql) throws SQLException {
        return false;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return null;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return 0;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {

    }

    @Override
    public int getFetchDirection() throws SQLException {
        return 0;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {

    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return 0;
    }

    @Override
    public void addBatch(String sql) throws SQLException {

    }

    @Override
    public void clearBatch() throws SQLException {

    }

    @Override
    public int[] executeBatch() throws SQLException {
        return new int[0];
    }

    @Override
    public Connection getConnection() throws SQLException {
        return null;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return null;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return 0;
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return false;
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return false;
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {

    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {

    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
