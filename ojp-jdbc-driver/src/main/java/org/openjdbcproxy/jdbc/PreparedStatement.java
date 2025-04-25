package org.openjdbcproxy.jdbc;

import com.openjdbcproxy.grpc.LobReference;
import com.openjdbcproxy.grpc.LobType;
import com.openjdbcproxy.grpc.OpResult;
import lombok.extern.slf4j.Slf4j;
import org.openjdbcproxy.constants.CommonConstants;
import org.openjdbcproxy.grpc.client.StatementService;
import org.openjdbcproxy.grpc.dto.Parameter;

import java.io.BufferedInputStream;
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
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.openjdbcproxy.grpc.SerializationHandler.deserialize;
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
    private StatementService statementService;
    private String uuid;//If present represents the UUID of this PreparedStatement in the server


    public PreparedStatement(Connection connection, String sql, StatementService statementService) {
        this.connection = connection;
        this.sql = sql;
        this.paramsMap = new TreeMap<>();
        this.statementService = statementService;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        Iterator<OpResult> itOpResult = this.statementService
                .executeQuery(this.connection.getSession(), this.sql, this.paramsMap.values().stream().toList());
        return new ResultSet(itOpResult, this.statementService, this);
    }

    @Override
    public int executeUpdate() throws SQLException {
        log.info("Executing update for -> {}", this.sql);
        OpResult result = this.statementService.executeUpdate(this.connection.getSession(), this.sql,
                this.paramsMap.values().stream().toList(), this.uuid);
        this.connection.setSession(result.getSession());
        return deserialize(result.getValue().toByteArray(), Integer.class);
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
                        .values(List.of(x))
                        .build());
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(BYTE)
                        .index(parameterIndex)
                        .values(List.of(new byte[]{x}))//Transform to byte array as it becomes an Object facilitating serialization.
                        .build());
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(SHORT)
                        .index(parameterIndex)
                        .values(List.of(x))
                        .build());
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(INT)
                        .index(parameterIndex)
                        .values(List.of(x))
                        .build());
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(LONG)
                        .index(parameterIndex)
                        .values(List.of(x))
                        .build());
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(FLOAT)
                        .index(parameterIndex)
                        .values(List.of(x))
                        .build());
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(DOUBLE)
                        .index(parameterIndex)
                        .values(List.of(x))
                        .build());
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(BIG_DECIMAL)
                        .index(parameterIndex)
                        .values(List.of(x))
                        .build());
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(STRING)
                        .index(parameterIndex)
                        .values(List.of(x))
                        .build());
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(BYTES)
                        .index(parameterIndex)
                        .values(List.of(x))
                        .build());
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(DATE)
                        .index(parameterIndex)
                        .values(List.of(x))
                        .build());
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(TIME)
                        .index(parameterIndex)
                        .values(List.of(x))
                        .build());
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(TIMESTAMP)
                        .index(parameterIndex)
                        .values(List.of(x))
                        .build());
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(ASCII_STREAM)
                        .index(parameterIndex)
                        .values(List.of(x))
                        .build());
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(UNICODE_STREAM)
                        .index(parameterIndex)
                        .values(List.of(x))
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
                        .values(List.of(x, targetSqlType))
                        .build());
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(OBJECT)
                        .index(parameterIndex)
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
                        .index(parameterIndex)
                        .values(List.of(reader, length))
                        .build());
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(REF)
                        .index(parameterIndex)
                        .values(List.of(x))
                        .build());
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        String blobUUID = ((org.openjdbcproxy.jdbc.Blob) x).getUUID();
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(BLOB)
                        .index(parameterIndex)
                        .values(List.of(blobUUID)) //Only send the Id as per the blob has been streamed in advance.
                        .build());
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        String clobUUID = ((org.openjdbcproxy.jdbc.Clob) x).getUUID();
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(CLOB)
                        .index(parameterIndex)
                        .values(List.of(clobUUID))
                        .build());
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(ARRAY)
                        .index(parameterIndex)
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
                        .index(parameterIndex)
                        .values(List.of(x, cal))
                        .build());
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(TIME)
                        .index(parameterIndex)
                        .values(List.of(x, cal))
                        .build());
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(TIMESTAMP)
                        .index(parameterIndex)
                        .values(List.of(x, cal))
                        .build());
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(NULL)
                        .index(parameterIndex)
                        .values(List.of(sqlType, typeName))
                        .build());
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(URL)
                        .index(parameterIndex)
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
                        .index(parameterIndex)
                        .values(List.of(x))
                        .build());
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(N_STRING)
                        .index(parameterIndex)
                        .values(List.of(value))
                        .build());
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        //TODO see if can use similar/same reader communication layer as other methods that require reader
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(N_CHARACTER_STREAM)
                        .index(parameterIndex)
                        .values(List.of(value, length))
                        .build());
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(N_CLOB)
                        .index(parameterIndex)
                        .values(List.of(value))
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
                            .values(List.of(clob.getUUID()))
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
                            .values(List.of(blob.getUUID()))
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
                        .values(List.of(reader, length))
                        .build());
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(SQL_XML)
                        .index(parameterIndex)
                        .values(List.of(xmlObject))
                        .build());
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(OBJECT)
                        .index(parameterIndex)
                        .values(List.of(x, targetSqlType, scaleOrLength))
                        .build());
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        this.paramsMap.put(parameterIndex,
                Parameter.builder()
                        .type(ASCII_STREAM)
                        .index(parameterIndex)
                        .values(List.of(x, length))
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
            metadata.put(CommonConstants.PREPARED_STATEMENT_UUID_BINARY_STREAM, this.uuid);
            LobReference lobReference = binaryStream.sendBinaryStream(LobType.LT_BINARY_STREAM, is, metadata);
            this.uuid = lobReference.getUuid();//Lob reference UUID for binary streams is the prepared statement uuid.
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
        return this.connection;
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
