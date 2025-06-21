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
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
public class PreparedStatement extends Statement implements java.sql.PreparedStatement {
    private final Connection connection;
    private String sql;
    private SortedMap<Integer, Parameter> paramsMap;
    private Map<String, Object> properties;
    private StatementService statementService;

    public PreparedStatement(Connection connection, String sql, StatementService statementService) {
        super(connection, statementService, null, ResourceType.RES_PREPARED_STATEMENT);
        this.connection = connection;
        this.sql = sql;
        this.properties = null;
        this.paramsMap = new TreeMap<>();
        this.statementService = statementService;
    }

    public PreparedStatement(Connection connection, String sql, StatementService statementService,
                             Map<String, Object> properties) {
        super(connection, statementService, properties, ResourceType.RES_PREPARED_STATEMENT);
        this.connection = connection;
        this.sql = sql;
        this.properties = properties;
        this.paramsMap = new TreeMap<>();
        this.statementService = statementService;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        this.checkClosed();
        Iterator<OpResult> itOpResult = this.statementService
                .executeQuery(this.connection.getSession(), this.sql, this.paramsMap.values().stream().toList(), this.properties);
        return new ResultSet(itOpResult, this.statementService, this);
    }

    @Override
    public int executeUpdate() throws SQLException {
        this.checkClosed();
        log.info("Executing update for -> {}", this.sql);
        OpResult result = this.statementService.executeUpdate(this.connection.getSession(), this.sql,
                this.paramsMap.values().stream().toList(), this.getStatementUUID(), null);
        this.connection.setSession(result.getSession());
        return deserialize(result.getValue().toByteArray(), Integer.class);
    }

    @Override
    public void addBatch() throws SQLException {
        this.checkClosed();
        log.info("Executing add batch for -> {}", this.sql);
        Map<String, Object> properties = new HashMap<>();
        properties.put(CommonConstants.PREPARED_STATEMENT_ADD_BATCH_FLAG, Boolean.TRUE);
        OpResult result = this.statementService.executeUpdate(this.connection.getSession(), this.sql,
                this.paramsMap.values().stream().toList(), this.getStatementUUID(), properties);
        this.connection.setSession(result.getSession());
        if (StringUtils.isBlank(this.getStatementUUID()) && ResultType.UUID_STRING.equals(result.getType()) &&
                !result.getValue().isEmpty()) {
            String psUUID = deserialize(result.getValue().toByteArray(), String.class);
            this.setStatementUUID(psUUID);
        }
        this.paramsMap = new TreeMap<>();
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        this.checkClosed();
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(NULL)
                        .index(parameterIndex)
                        .build());
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        this.checkClosed();
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(BOOLEAN)
                        .index(parameterIndex)
                        .values(Arrays.asList(x))
                        .build());
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        this.checkClosed();
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(BYTE)
                        .index(parameterIndex)
                        .values(Arrays.asList(new byte[]{x}))//Transform to byte array as it becomes an Object facilitating serialization.
                        .build());
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        this.checkClosed();
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(SHORT)
                        .index(parameterIndex)
                        .values(Arrays.asList(x))
                        .build());
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        this.checkClosed();
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(INT)
                        .index(parameterIndex)
                        .values(Arrays.asList(x))
                        .build());
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        this.checkClosed();
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(LONG)
                        .index(parameterIndex)
                        .values(Arrays.asList(x))
                        .build());
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        this.checkClosed();
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(FLOAT)
                        .index(parameterIndex)
                        .values(Arrays.asList(x))
                        .build());
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        this.checkClosed();
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(DOUBLE)
                        .index(parameterIndex)
                        .values(Arrays.asList(x))
                        .build());
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        this.checkClosed();
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(BIG_DECIMAL)
                        .index(parameterIndex)
                        .values(Arrays.asList(x))
                        .build());
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        this.checkClosed();
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(STRING)
                        .index(parameterIndex)
                        .values(Arrays.asList(x))
                        .build());
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        this.checkClosed();
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(BYTES)
                        .index(parameterIndex)
                        .values(Arrays.asList(x))
                        .build());
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        this.checkClosed();
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(DATE)
                        .index(parameterIndex)
                        .values(Arrays.asList(x))
                        .build());
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        this.checkClosed();
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(TIME)
                        .index(parameterIndex)
                        .values(Arrays.asList(x))
                        .build());
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        this.checkClosed();
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(TIMESTAMP)
                        .index(parameterIndex)
                        .values(Arrays.asList(x))
                        .build());
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        this.checkClosed();
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(ASCII_STREAM)
                        .index(parameterIndex)
                        .values(Arrays.asList(x))
                        .build());
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        this.checkClosed();
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(UNICODE_STREAM)
                        .index(parameterIndex)
                        .values(Arrays.asList(x))
                        .build());
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream inputStream, int length) throws SQLException {
        this.checkClosed();
        this.setBinaryStream(parameterIndex, inputStream, (long) length);
    }

    @Override
    public void clearParameters() throws SQLException {
        this.checkClosed();
        this.paramsMap = new TreeMap<>();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        this.checkClosed();
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(OBJECT)
                        .index(parameterIndex)
                        .values(Arrays.asList(x, targetSqlType))
                        .build());
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        this.checkClosed();
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(OBJECT)
                        .index(parameterIndex)
                        .values(Arrays.asList(x))
                        .build());
    }

    @Override
    public boolean execute() throws SQLException {
        this.checkClosed();
        String trimmedSql = sql.trim().toUpperCase();
        if (trimmedSql.startsWith("SELECT")) {
            // Delegate to executeQuery
            ResultSet resultSet = this.executeQuery();
            // Store the ResultSet for later retrieval if needed
            this.lastResultSet = resultSet;
            this.lastUpdateCount = -1;
            return true; // Indicates a ResultSet was returned
        } else {
            // Delegate to executeUpdate
            this.lastUpdateCount = this.executeUpdate();
            return false; // Indicates no ResultSet was returned
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        this.checkClosed();
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
        this.checkClosed();
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
        this.checkClosed();
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
        this.checkClosed();
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
        this.checkClosed();
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(ARRAY)
                        .index(parameterIndex)
                        .values(Arrays.asList(x))
                        .build());
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        this.checkClosed();
        return new org.openjdbcproxy.jdbc.ResultSetMetaData(this, this.statementService);
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        this.checkClosed();
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(DATE)
                        .index(parameterIndex)
                        .values(Arrays.asList(x, cal))
                        .build());
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        this.checkClosed();
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(TIME)
                        .index(parameterIndex)
                        .values(Arrays.asList(x, cal))
                        .build());
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        this.checkClosed();
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(TIMESTAMP)
                        .index(parameterIndex)
                        .values(Arrays.asList(x, cal))
                        .build());
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        this.checkClosed();
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(NULL)
                        .index(parameterIndex)
                        .values(Arrays.asList(sqlType, typeName))
                        .build());
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        this.checkClosed();
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
        this.checkClosed();
        return new org.openjdbcproxy.jdbc.ParameterMetaData();
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        this.checkClosed();
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
        this.checkClosed();
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(N_STRING)
                        .index(parameterIndex)
                        .values(Arrays.asList(value))
                        .build());
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        this.checkClosed();
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
        this.checkClosed();
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(N_CLOB)
                        .index(parameterIndex)
                        .values(Arrays.asList(value))
                        .build());
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        this.checkClosed();
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
        this.checkClosed();
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
        this.checkClosed();
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
        this.checkClosed();
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(SQL_XML)
                        .index(parameterIndex)
                        .values(Arrays.asList(xmlObject))
                        .build());
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        this.checkClosed();
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(OBJECT)
                        .index(parameterIndex)
                        .values(Arrays.asList(x, targetSqlType, scaleOrLength))
                        .build());
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        this.checkClosed();
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(ASCII_STREAM)
                        .index(parameterIndex)
                        .values(Arrays.asList(x, length))
                        .build());
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream is, long length) throws SQLException {
        this.checkClosed();
        try {
            BinaryStream binaryStream = new BinaryStream(this.getConnection(),
                    new LobServiceImpl(this.connection, this.statementService),
                    this.statementService, null);
            Map<Integer, Object> metadata = new HashMap<>();
            metadata.put(CommonConstants.PREPARED_STATEMENT_BINARY_STREAM_INDEX, parameterIndex);
            metadata.put(CommonConstants.PREPARED_STATEMENT_BINARY_STREAM_LENGTH, length);
            metadata.put(CommonConstants.PREPARED_STATEMENT_BINARY_STREAM_SQL, this.sql);
            metadata.put(CommonConstants.PREPARED_STATEMENT_UUID_BINARY_STREAM, this.getStatementUUID());
            LobReference lobReference = binaryStream.sendBinaryStream(LobType.LT_BINARY_STREAM, is, metadata);
            this.setStatementUUID(lobReference.getUuid());//Lob reference UUID for binary streams is the prepared statement uuid.
        } catch (RuntimeException e) {
            throw new SQLException("Unable to write binary stream: " + e.getMessage(), e);
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        this.checkClosed();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        this.checkClosed();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        this.checkClosed();
        this.setBinaryStream(parameterIndex, x, -1); //-1 means not provided in OJP BynaryStrem server side
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        this.checkClosed();
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        this.checkClosed();
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        this.checkClosed();
        this.setClob(parameterIndex, reader, Long.MAX_VALUE);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        this.setBlob(parameterIndex, inputStream, Integer.MAX_VALUE);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        this.checkClosed();
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

    private CallResourceRequest.Builder newCallBuilder() throws SQLException {
        this.propertiesHaveSqlStatement();
        CallResourceRequest.Builder builder = CallResourceRequest.newBuilder()
                .setSession(this.connection.getSession())
                .setResourceType(ResourceType.RES_PREPARED_STATEMENT);
        if (this.getStatementUUID() != null) {
            builder.setResourceUUID(this.getStatementUUID());
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
        if (this.getStatementUUID() == null && !response.getResourceUUID().isBlank()) {
            this.setStatementUUID(response.getResourceUUID());
        }
        if (Void.class.equals(returnType)) {
            return null;
        }
        return (T) deserialize(response.getValues().toByteArray(), returnType);
    }
}