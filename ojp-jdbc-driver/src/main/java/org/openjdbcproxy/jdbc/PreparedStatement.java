package org.openjdbcproxy.jdbc;

import com.google.protobuf.ByteString;
import com.openjdbcproxy.grpc.CallResourceRequest;
import com.openjdbcproxy.grpc.CallResourceResponse;
import com.openjdbcproxy.grpc.CallType;
import com.openjdbcproxy.grpc.LobReference;
import com.openjdbcproxy.grpc.LobType;
import com.openjdbcproxy.grpc.OpResult;
import com.openjdbcproxy.grpc.ResourceType;
import com.openjdbcproxy.grpc.ResultType;
import com.openjdbcproxy.grpc.TargetCall;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.openjdbcproxy.constants.CommonConstants;
import org.openjdbcproxy.grpc.client.StatementService;
import org.openjdbcproxy.grpc.dto.Parameter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.openjdbcproxy.grpc.SerializationHandler.deserialize;
import static org.openjdbcproxy.grpc.SerializationHandler.serialize;
import static org.openjdbcproxy.grpc.dto.ParameterType.ARRAY;
import static org.openjdbcproxy.grpc.dto.ParameterType.ASCII_STREAM;
import static org.openjdbcproxy.grpc.dto.ParameterType.BIG_DECIMAL;
import static org.openjdbcproxy.grpc.dto.ParameterType.BLOB;
import static org.openjdbcproxy.grpc.dto.ParameterType.BOOLEAN;
import static org.openjdbcproxy.grpc.dto.ParameterType.BYTE;
import static org.openjdbcproxy.grpc.dto.ParameterType.BYTES;
import static org.openjdbcproxy.grpc.dto.ParameterType.CHARACTER_READER;
import static org.openjdbcproxy.grpc.dto.ParameterType.CLOB;
import static org.openjdbcproxy.grpc.dto.ParameterType.DATE;
import static org.openjdbcproxy.grpc.dto.ParameterType.DOUBLE;
import static org.openjdbcproxy.grpc.dto.ParameterType.FLOAT;
import static org.openjdbcproxy.grpc.dto.ParameterType.INT;
import static org.openjdbcproxy.grpc.dto.ParameterType.LONG;
import static org.openjdbcproxy.grpc.dto.ParameterType.NULL;
import static org.openjdbcproxy.grpc.dto.ParameterType.N_CHARACTER_STREAM;
import static org.openjdbcproxy.grpc.dto.ParameterType.N_CLOB;
import static org.openjdbcproxy.grpc.dto.ParameterType.N_STRING;
import static org.openjdbcproxy.grpc.dto.ParameterType.OBJECT;
import static org.openjdbcproxy.grpc.dto.ParameterType.REF;
import static org.openjdbcproxy.grpc.dto.ParameterType.ROW_ID;
import static org.openjdbcproxy.grpc.dto.ParameterType.SHORT;
import static org.openjdbcproxy.grpc.dto.ParameterType.SQL_XML;
import static org.openjdbcproxy.grpc.dto.ParameterType.STRING;
import static org.openjdbcproxy.grpc.dto.ParameterType.TIME;
import static org.openjdbcproxy.grpc.dto.ParameterType.TIMESTAMP;
import static org.openjdbcproxy.grpc.dto.ParameterType.UNICODE_STREAM;
import static org.openjdbcproxy.grpc.dto.ParameterType.URL;

@Slf4j
public class PreparedStatement implements java.sql.PreparedStatement {
    private final Connection connection;
    private String sql;
    private SortedMap<Integer, Parameter> paramsMap;
    private Map<String, Object> properties;
    private StatementService statementService;
    @Getter
    private String prepareStatementUUID;//If present represents the UUID of this PreparedStatement in the server
    private Statement statement;

    public PreparedStatement(Connection connection, String sql, StatementService statementService) {
        this.connection = connection;
        this.sql = sql;
        this.properties = null;
        this.paramsMap = new TreeMap<>();
        this.statementService = statementService;
    }

    public PreparedStatement(Connection connection, String sql, StatementService statementService,
                             Map<String, Object> properties) {
        this.connection = connection;
        this.sql = sql;
        this.properties = properties;
        this.paramsMap = new TreeMap<>();
        this.statementService = statementService;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        Iterator<OpResult> itOpResult = this.statementService
                .executeQuery(this.connection.getSession(), this.sql, this.paramsMap.values().stream().toList(), this.properties);
        return new ResultSet(itOpResult, this.statementService, this);
    }

    @Override
    public int executeUpdate() throws SQLException {
        log.info("Executing update for -> {}", this.sql);
        OpResult result = this.statementService.executeUpdate(this.connection.getSession(), this.sql,
                this.paramsMap.values().stream().toList(), this.prepareStatementUUID, null);
        this.connection.setSession(result.getSession());
        return deserialize(result.getValue().toByteArray(), Integer.class);
    }

    @Override
    public void addBatch() throws SQLException {
        log.info("Executing add batch for -> {}", this.sql);
        Map<String, Object> properties = new HashMap<>();
        properties.put(CommonConstants.PREPARED_STATEMENT_ADD_BATCH_FLAG, Boolean.TRUE);
        OpResult result = this.statementService.executeUpdate(this.connection.getSession(), this.sql,
                this.paramsMap.values().stream().toList(), this.prepareStatementUUID, properties);
        this.connection.setSession(result.getSession());
        if (StringUtils.isBlank(this.prepareStatementUUID) && ResultType.UUID_STRING.equals(result.getType()) &&
            !result.getValue().isEmpty()) {
            String psUUID = deserialize(result.getValue().toByteArray(), String.class);
            this.prepareStatementUUID = psUUID;
            this.getStatement().setStatementUUID(psUUID);
        }
        this.paramsMap = new TreeMap<>();
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(NULL)
                        .index(parameterIndex)
                        .build());
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(BOOLEAN)
                        .index(parameterIndex)
                        .values(Arrays.asList(x))
                        .build());
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(BYTE)
                        .index(parameterIndex)
                        .values(Arrays.asList(new byte[]{x}))//Transform to byte array as it becomes an Object facilitating serialization.
                        .build());
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(SHORT)
                        .index(parameterIndex)
                        .values(Arrays.asList(x))
                        .build());
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(INT)
                        .index(parameterIndex)
                        .values(Arrays.asList(x))
                        .build());
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(LONG)
                        .index(parameterIndex)
                        .values(Arrays.asList(x))
                        .build());
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(FLOAT)
                        .index(parameterIndex)
                        .values(Arrays.asList(x))
                        .build());
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(DOUBLE)
                        .index(parameterIndex)
                        .values(Arrays.asList(x))
                        .build());
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(BIG_DECIMAL)
                        .index(parameterIndex)
                        .values(Arrays.asList(x))
                        .build());
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(STRING)
                        .index(parameterIndex)
                        .values(Arrays.asList(x))
                        .build());
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(BYTES)
                        .index(parameterIndex)
                        .values(Arrays.asList(x))
                        .build());
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(DATE)
                        .index(parameterIndex)
                        .values(Arrays.asList(x))
                        .build());
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(TIME)
                        .index(parameterIndex)
                        .values(Arrays.asList(x))
                        .build());
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(TIMESTAMP)
                        .index(parameterIndex)
                        .values(Arrays.asList(x))
                        .build());
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(ASCII_STREAM)
                        .index(parameterIndex)
                        .values(Arrays.asList(x))
                        .build());
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(UNICODE_STREAM)
                        .index(parameterIndex)
                        .values(Arrays.asList(x))
                        .build());
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream inputStream, int length) throws SQLException {
        this.setBinaryStream(parameterIndex, inputStream, (long) length);
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
                        .index(parameterIndex)
                        .values(Arrays.asList(x, targetSqlType))
                        .build());
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(OBJECT)
                        .index(parameterIndex)
                        .values(Arrays.asList(x))
                        .build());
    }

    @Override
    public boolean execute() throws SQLException {
        throw new SQLException("Not supported.");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        //TODO this will require an implementation of Reader that communicates across GRPC
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(CHARACTER_READER)
                        .index(parameterIndex)
                        .values(Arrays.asList(reader, length))
                        .build());
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        if (DbInfo.isH2DB()) {
            throw new SQLException("Not supported.");
        }
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(REF)
                        .index(parameterIndex)
                        .values(Arrays.asList(x))
                        .build());
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        String blobUUID = (x != null) ? ((org.openjdbcproxy.jdbc.Blob) x).getUUID() : null;
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(BLOB)
                        .index(parameterIndex)
                        .values(Arrays.asList(blobUUID)) //Only send the Id as per the blob has been streamed in advance.
                        .build());
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        String clobUUID = (x != null) ? ((org.openjdbcproxy.jdbc.Clob) x).getUUID() : null;
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(CLOB)
                        .index(parameterIndex)
                        .values(Arrays.asList(clobUUID))
                        .build());
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(ARRAY)
                        .index(parameterIndex)
                        .values(Arrays.asList(x))
                        .build());
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new org.openjdbcproxy.jdbc.ResultSetMetaData(this, this.statementService);
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(DATE)
                        .index(parameterIndex)
                        .values(Arrays.asList(x, cal))
                        .build());
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(TIME)
                        .index(parameterIndex)
                        .values(Arrays.asList(x, cal))
                        .build());
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(TIMESTAMP)
                        .index(parameterIndex)
                        .values(Arrays.asList(x, cal))
                        .build());
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(NULL)
                        .index(parameterIndex)
                        .values(Arrays.asList(sqlType, typeName))
                        .build());
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        if (DbInfo.isH2DB()) {
            throw new SQLException("Not supported.");
        }
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(URL)
                        .index(parameterIndex)
                        .values(Arrays.asList(x))
                        .build());
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return new org.openjdbcproxy.jdbc.ParameterMetaData();
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        if (DbInfo.isH2DB()) {
            throw new SQLException("Not supported.");
        }
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(ROW_ID)
                        .index(parameterIndex)
                        .values(Arrays.asList(x))
                        .build());
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(N_STRING)
                        .index(parameterIndex)
                        .values(Arrays.asList(value))
                        .build());
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        //TODO see if can use similar/same reader communication layer as other methods that require reader
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(N_CHARACTER_STREAM)
                        .index(parameterIndex)
                        .values(Arrays.asList(value, length))
                        .build());
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(N_CLOB)
                        .index(parameterIndex)
                        .values(Arrays.asList(value))
                        .build());
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        try {
            org.openjdbcproxy.jdbc.Clob clob = (org.openjdbcproxy.jdbc.Clob) this.getConnection().createClob();
            OutputStream os = clob.setAsciiStream(1);
            int byteRead = reader.read();
            int writtenLength = 0;
            while (byteRead != -1 && length > writtenLength) {
                os.write(byteRead);
                writtenLength++;
                byteRead = reader.read();
            }
            os.close();
            this.paramsMap.put(parameterIndex,
                    Parameter.builder()
                            .type(CLOB)
                            .index(parameterIndex)
                            .values(Arrays.asList(clob.getUUID()))
                            .build()
            );
        } catch (IOException e) {
            throw new SQLException("Unable to write CLOB bytes: " + e.getMessage(), e);
        }
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        try {
            org.openjdbcproxy.jdbc.Blob blob = (org.openjdbcproxy.jdbc.Blob) this.getConnection().createBlob();
            OutputStream os = blob.setBinaryStream(1);
            int byteRead = inputStream.read();
            int writtenLength = 0;
            while (byteRead != -1 && length > writtenLength) {
                os.write(byteRead);
                writtenLength++;
                byteRead = inputStream.read();
            }
            os.close();
            this.paramsMap.put(parameterIndex,
                    Parameter.builder()
                            .type(BLOB)
                            .index(parameterIndex)
                            .values(Arrays.asList(blob.getUUID()))
                            .build()
            );
        } catch (IOException e) {
            throw new SQLException("Unable to write BLOB bytes: " + e.getMessage(), e);
        }
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        //TODO see if can use similar/same reader communication layer as other methods that require reader
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(N_CLOB)
                        .index(parameterIndex)
                        .values(Arrays.asList(reader, length))
                        .build());
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(SQL_XML)
                        .index(parameterIndex)
                        .values(Arrays.asList(xmlObject))
                        .build());
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(OBJECT)
                        .index(parameterIndex)
                        .values(Arrays.asList(x, targetSqlType, scaleOrLength))
                        .build());
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(ASCII_STREAM)
                        .index(parameterIndex)
                        .values(Arrays.asList(x, length))
                        .build());
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream is, long length) throws SQLException {
        try {
            BinaryStream binaryStream = new BinaryStream(this.getConnection(),
                    new LobServiceImpl(this.connection, this.statementService),
                    this.statementService, null);
            Map<Integer, Object> metadata = new HashMap<>();
            metadata.put(CommonConstants.PREPARED_STATEMENT_BINARY_STREAM_INDEX, parameterIndex);
            metadata.put(CommonConstants.PREPARED_STATEMENT_BINARY_STREAM_LENGTH, length);
            metadata.put(CommonConstants.PREPARED_STATEMENT_BINARY_STREAM_SQL, this.sql);
            metadata.put(CommonConstants.PREPARED_STATEMENT_UUID_BINARY_STREAM, this.prepareStatementUUID);
            LobReference lobReference = binaryStream.sendBinaryStream(LobType.LT_BINARY_STREAM, is, metadata);
            this.prepareStatementUUID = lobReference.getUuid();//Lob reference UUID for binary streams is the prepared statement uuid.
        } catch (RuntimeException e) {
            throw new SQLException("Unable to write binary stream: " + e.getMessage(), e);
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        this.setBinaryStream(parameterIndex, x, -1); //-1 means not provided in OJP BynaryStrem server side
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {

    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {

    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        this.setClob(parameterIndex, reader, Long.MAX_VALUE);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        this.setBlob(parameterIndex, inputStream, Integer.MAX_VALUE);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {

    }

    // --- Statement interface methods (delegated to this.getStatement()) ---

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        return this.getStatement().executeQuery(sql);
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        return this.getStatement().executeUpdate(sql);
    }

    @Override
    public void close() throws SQLException {
        this.getStatement().close();
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return this.getStatement().getMaxFieldSize();
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        this.getStatement().setMaxFieldSize(max);
    }

    @Override
    public int getMaxRows() throws SQLException {
        return this.getStatement().getMaxRows();
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        this.getStatement().setMaxRows(max);
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        this.getStatement().setEscapeProcessing(enable);
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return this.getStatement().getQueryTimeout();
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        this.getStatement().setQueryTimeout(seconds);
    }

    @Override
    public void cancel() throws SQLException {
        this.getStatement().cancel();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return this.getStatement().getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        this.getStatement().clearWarnings();
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        this.getStatement().setCursorName(name);
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        return this.getStatement().execute(sql);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return this.getStatement().getResultSet();
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return this.getStatement().getUpdateCount();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return this.getStatement().getMoreResults();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        this.getStatement().setFetchDirection(direction);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return this.getStatement().getFetchDirection();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        this.getStatement().setFetchSize(rows);
    }

    @Override
    public int getFetchSize() throws SQLException {
        return this.getStatement().getFetchSize();
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return this.getStatement().getResultSetConcurrency();
    }

    @Override
    public int getResultSetType() throws SQLException {
        return this.getStatement().getResultSetType();
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        this.getStatement().addBatch(sql);
    }

    @Override
    public void clearBatch() throws SQLException {
        this.getStatement().clearBatch();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return this.getStatement().executeBatch();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return this.connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return this.getStatement().getMoreResults(current);
    }

    @Override
    public RemoteProxyResultSet getGeneratedKeys() throws SQLException {
        return this.getStatement().getGeneratedKeys();
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return this.getStatement().executeUpdate(sql, autoGeneratedKeys);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return this.getStatement().executeUpdate(sql, columnIndexes);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return this.getStatement().executeUpdate(sql, columnNames);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return this.getStatement().execute(sql, autoGeneratedKeys);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return this.getStatement().execute(sql, columnIndexes);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return this.getStatement().execute(sql, columnNames);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return this.getStatement().getResultSetHoldability();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.getStatement().isClosed();
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        this.getStatement().setPoolable(poolable);
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return this.getStatement().isPoolable();
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        this.getStatement().closeOnCompletion();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return this.getStatement().isCloseOnCompletion();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    private synchronized Statement getStatement() throws SQLException {
        if (this.statement == null) {
            this.propertiesHaveSqlStatement();
            this.statement = new Statement(this.connection, this.statementService, this.properties,
                    ResourceType.RES_PREPARED_STATEMENT);
        }
        return this.statement;
    }

    public Map<String, Object> getProperties() {
        this.propertiesHaveSqlStatement();
        return this.properties;
    }

    /**
     * Guarantees that the properties map has the sql statement set in this prepared statement.
     */
    private void propertiesHaveSqlStatement() {
        String sqlProperty = (this.properties != null &&
                this.properties.get(CommonConstants.PREPARED_STATEMENT_SQL_KEY) != null) ?
                this.properties.get(CommonConstants.PREPARED_STATEMENT_SQL_KEY).toString() : null;
        if (StringUtils.isBlank(sqlProperty) && StringUtils.isNotBlank(this.sql)) {
            if (this.properties == null) {
                this.properties = new HashMap<>();
            }
            this.properties.put(CommonConstants.PREPARED_STATEMENT_SQL_KEY, this.sql);
        }
    }

    private CallResourceRequest.Builder newCallBuilder() {
        this.propertiesHaveSqlStatement();
        CallResourceRequest.Builder builder = CallResourceRequest.newBuilder()
                .setSession(this.connection.getSession())
                .setResourceType(ResourceType.RES_PREPARED_STATEMENT);
        if (this.prepareStatementUUID != null) {
            builder.setResourceUUID(this.prepareStatementUUID);
        }
        if (this.properties != null) {
            builder.setProperties(ByteString.copyFrom(serialize(this.properties)));
        }
        return builder;
    }

    private <T> T callProxy(CallType callType, String targetName, Class<?> returnType) throws SQLException {
        return this.callProxy(callType, targetName, returnType, Constants.EMPTY_OBJECT_LIST);
    }

    private <T> T callProxy(CallType callType, String targetName, Class<?> returnType, List<Object> params) throws SQLException {
        CallResourceRequest.Builder reqBuilder = this.newCallBuilder();
        reqBuilder.setTarget(
                TargetCall.newBuilder()
                        .setCallType(callType)
                        .setResourceName(targetName)
                        .setParams(ByteString.copyFrom(serialize(params)))
                        .build()
        );
        CallResourceResponse response = this.statementService.callResource(reqBuilder.build());
        this.connection.setSession(response.getSession());
        if (this.prepareStatementUUID == null && !response.getResourceUUID().isBlank()) {
            this.prepareStatementUUID = response.getResourceUUID();
        }
        if (Void.class.equals(returnType)) {
            return null;
        }
        return (T) deserialize(response.getValues().toByteArray(), returnType);
    }
}