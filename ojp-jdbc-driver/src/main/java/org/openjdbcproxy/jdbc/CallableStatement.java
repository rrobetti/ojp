package org.openjdbcproxy.jdbc;

import com.google.protobuf.ByteString;
import com.openjdbcproxy.grpc.CallResourceRequest;
import com.openjdbcproxy.grpc.CallResourceResponse;
import com.openjdbcproxy.grpc.CallType;
import com.openjdbcproxy.grpc.ResourceType;
import com.openjdbcproxy.grpc.TargetCall;
import io.grpc.StatusRuntimeException;
import lombok.AllArgsConstructor;
import org.openjdbcproxy.grpc.client.GrpcExceptionHandler;
import org.openjdbcproxy.grpc.client.StatementService;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import static org.openjdbcproxy.grpc.SerializationHandler.deserialize;
import static org.openjdbcproxy.grpc.SerializationHandler.serialize;

@AllArgsConstructor
public class CallableStatement implements java.sql.CallableStatement {
    private final org.openjdbcproxy.jdbc.Connection connection;
    private final StatementService statementService;
    private final String remoteCallableStatementUUID;

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
        this.callProxy(CallType.CALL_REGISTER, "OutParameter", Void.class, List.of(parameterIndex, sqlType));
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
        this.callProxy(CallType.CALL_REGISTER, "OutParameter", Void.class, List.of(parameterIndex, sqlType, scale));
    }

    @Override
    public boolean wasNull() throws SQLException {
        return this.callProxy(CallType.CALL_WAS, "Null", Boolean.class);
    }

    @Override
    public String getString(int parameterIndex) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "String", String.class, List.of(parameterIndex));
    }

    @Override
    public boolean getBoolean(int parameterIndex) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Boolean", Boolean.class, List.of(parameterIndex));
    }

    @Override
    public byte getByte(int parameterIndex) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Byte", Byte.class, List.of(parameterIndex));
    }

    @Override
    public short getShort(int parameterIndex) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Short", Short.class, List.of(parameterIndex));
    }

    @Override
    public int getInt(int parameterIndex) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Int", Integer.class, List.of(parameterIndex));
    }

    @Override
    public long getLong(int parameterIndex) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Long", Long.class, List.of(parameterIndex));
    }

    @Override
    public float getFloat(int parameterIndex) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Float", Float.class, List.of(parameterIndex));
    }

    @Override
    public double getDouble(int parameterIndex) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Double", Double.class, List.of(parameterIndex));
    }

    @Override
    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "BigDecimal", BigDecimal.class, List.of(parameterIndex, scale));
    }

    @Override
    public byte[] getBytes(int parameterIndex) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Bytes", byte[].class, List.of(parameterIndex));
    }

    @Override
    public Date getDate(int parameterIndex) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Date", Date.class, List.of(parameterIndex));
    }

    @Override
    public Time getTime(int parameterIndex) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Time", Time.class, List.of(parameterIndex));
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Timestamp", Timestamp.class, List.of(parameterIndex));
    }

    @Override
    public Object getObject(int parameterIndex) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Object", Object.class, List.of(parameterIndex));
    }

    @Override
    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "BigDecimal", BigDecimal.class, List.of(parameterIndex));
    }

    @Override
    public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Object", Object.class, List.of(parameterIndex, map));
    }

    @Override
    public Ref getRef(int parameterIndex) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Ref", Ref.class, List.of(parameterIndex));
    }

    @Override
    public Blob getBlob(int parameterIndex) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Blob", Blob.class, List.of(parameterIndex));
    }

    @Override
    public Clob getClob(int parameterIndex) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Clob", Clob.class, List.of(parameterIndex));
    }

    @Override
    public Array getArray(int parameterIndex) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Array", Array.class, List.of(parameterIndex));
    }

    @Override
    public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Date", Date.class, List.of(parameterIndex, cal));
    }

    @Override
    public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Time", Time.class, List.of(parameterIndex, cal));
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Timestamp", Timestamp.class, List.of(parameterIndex, cal));
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, String typeName) throws SQLException {
        this.callProxy(CallType.CALL_REGISTER, "OutParameter", Void.class, List.of(parameterIndex, sqlType, typeName));
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
        this.callProxy(CallType.CALL_REGISTER, "OutParameter", Void.class, List.of(parameterName, sqlType));
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException {
        this.callProxy(CallType.CALL_REGISTER, "OutParameter", Void.class, List.of(parameterName, sqlType, scale));
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException {
        this.callProxy(CallType.CALL_REGISTER, "OutParameter", Void.class, List.of(parameterName, sqlType, typeName));
    }

    @Override
    public URL getURL(int parameterIndex) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "URL", URL.class, List.of(parameterIndex));
    }

    @Override
    public void setURL(String parameterName, URL val) throws SQLException {
        this.callProxy(CallType.CALL_SET, "URL", Void.class, List.of(parameterName, val));
    }

    @Override
    public void setNull(String parameterName, int sqlType) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Null", Void.class, List.of(parameterName, sqlType));
    }

    @Override
    public void setBoolean(String parameterName, boolean x) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Boolean", Void.class, List.of(parameterName, x));
    }

    @Override
    public void setByte(String parameterName, byte x) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Byte", Void.class, List.of(parameterName, x));
    }

    @Override
    public void setShort(String parameterName, short x) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Short", Void.class, List.of(parameterName, x));
    }

    @Override
    public void setInt(String parameterName, int x) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Int", Void.class, List.of(parameterName, x));
    }

    @Override
    public void setLong(String parameterName, long x) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Long", Void.class, List.of(parameterName, x));
    }

    @Override
    public void setFloat(String parameterName, float x) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Float", Void.class, List.of(parameterName, x));
    }

    @Override
    public void setDouble(String parameterName, double x) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Double", Void.class, List.of(parameterName, x));
    }

    @Override
    public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
        this.callProxy(CallType.CALL_SET, "BigDecimal", Void.class, List.of(parameterName, x));
    }

    @Override
    public void setString(String parameterName, String x) throws SQLException {
        this.callProxy(CallType.CALL_SET, "String", Void.class, List.of(parameterName, x));
    }

    @Override
    public void setBytes(String parameterName, byte[] x) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Bytes", Void.class, List.of(parameterName, x));
    }

    @Override
    public void setDate(String parameterName, Date x) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Date", Void.class, List.of(parameterName, x));
    }

    @Override
    public void setTime(String parameterName, Time x) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Time", Void.class, List.of(parameterName, x));
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Timestamp", Void.class, List.of(parameterName, x));
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
        this.callProxy(CallType.CALL_SET, "AsciiStream", Void.class, List.of(parameterName, x, length));
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
        this.callProxy(CallType.CALL_SET, "BinaryStream", Void.class, List.of(parameterName, x, length));
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Object", Void.class, List.of(parameterName, x, targetSqlType, scale));
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Object", Void.class, List.of(parameterName, x, targetSqlType));
    }

    @Override
    public void setObject(String parameterName, Object x) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Object", Void.class, List.of(parameterName, x));
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
        this.callProxy(CallType.CALL_SET, "CharacterStream", Void.class, List.of(parameterName, reader, length));
    }

    @Override
    public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Date", Void.class, List.of(parameterName, x, cal));
    }

    @Override
    public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Time", Void.class, List.of(parameterName, x, cal));
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Timestamp", Void.class, List.of(parameterName, x, cal));
    }

    @Override
    public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Null", Void.class, List.of(parameterName, sqlType, typeName));
    }

    @Override
    public String getString(String parameterName) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "String", String.class, List.of(parameterName));
    }

    @Override
    public boolean getBoolean(String parameterName) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Boolean", Boolean.class, List.of(parameterName));
    }

    @Override
    public byte getByte(String parameterName) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Byte", Byte.class, List.of(parameterName));
    }

    @Override
    public short getShort(String parameterName) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Short", Short.class, List.of(parameterName));
    }

    @Override
    public int getInt(String parameterName) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Int", Integer.class, List.of(parameterName));
    }

    @Override
    public long getLong(String parameterName) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Long", Long.class, List.of(parameterName));
    }

    @Override
    public float getFloat(String parameterName) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Float", Float.class, List.of(parameterName));
    }

    @Override
    public double getDouble(String parameterName) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Double", Double.class, List.of(parameterName));
    }

    @Override
    public byte[] getBytes(String parameterName) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Bytes", byte[].class, List.of(parameterName));
    }

    @Override
    public Date getDate(String parameterName) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Date", Date.class, List.of(parameterName));
    }

    @Override
    public Time getTime(String parameterName) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Time", Time.class, List.of(parameterName));
    }

    @Override
    public Timestamp getTimestamp(String parameterName) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Timestamp", Timestamp.class, List.of(parameterName));
    }

    @Override
    public Object getObject(String parameterName) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Object", Object.class, List.of(parameterName));
    }

    @Override
    public BigDecimal getBigDecimal(String parameterName) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "BigDecimal", BigDecimal.class, List.of(parameterName));
    }

    @Override
    public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Object", Object.class, List.of(parameterName, map));
    }

    @Override
    public Ref getRef(String parameterName) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Ref", Ref.class, List.of(parameterName));
    }

    @Override
    public Blob getBlob(String parameterName) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Blob", Blob.class, List.of(parameterName));
    }

    @Override
    public Clob getClob(String parameterName) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Clob", Clob.class, List.of(parameterName));
    }

    @Override
    public Array getArray(String parameterName) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Array", Array.class, List.of(parameterName));
    }

    @Override
    public Date getDate(String parameterName, Calendar cal) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Date", Date.class, List.of(parameterName, cal));
    }

    @Override
    public Time getTime(String parameterName, Calendar cal) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Time", Time.class, List.of(parameterName, cal));
    }

    @Override
    public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Timestamp", Timestamp.class, List.of(parameterName, cal));
    }

    @Override
    public URL getURL(String parameterName) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "URL", URL.class, List.of(parameterName));
    }

    @Override
    public RowId getRowId(int parameterIndex) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "RowId", RowId.class, List.of(parameterIndex));
    }

    @Override
    public RowId getRowId(String parameterName) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "RowId", RowId.class, List.of(parameterName));
    }

    @Override
    public void setRowId(String parameterName, RowId x) throws SQLException {
        this.callProxy(CallType.CALL_SET, "RowId", Void.class, List.of(parameterName, x));
    }

    @Override
    public void setNString(String parameterName, String value) throws SQLException {
        this.callProxy(CallType.CALL_SET, "NString", Void.class, List.of(parameterName, value));
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {
        this.callProxy(CallType.CALL_SET, "NCharacterStream", Void.class, List.of(parameterName, value, length));
    }

    @Override
    public void setNClob(String parameterName, NClob value) throws SQLException {
        this.callProxy(CallType.CALL_SET, "NClob", Void.class, List.of(parameterName, value));
    }

    @Override
    public void setClob(String parameterName, Reader reader, long length) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Clob", Void.class, List.of(parameterName, reader, length));
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Blob", Void.class, List.of(parameterName, inputStream, length));
    }

    @Override
    public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
        this.callProxy(CallType.CALL_SET, "NClob", Void.class, List.of(parameterName, reader, length));
    }

    @Override
    public NClob getNClob(int parameterIndex) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "NClob", NClob.class, List.of(parameterIndex));
    }

    @Override
    public NClob getNClob(String parameterName) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "NClob", NClob.class, List.of(parameterName));
    }

    @Override
    public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
        this.callProxy(CallType.CALL_SET, "SQLXML", Void.class, List.of(parameterName, xmlObject));
    }

    @Override
    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "SQLXML", SQLXML.class, List.of(parameterIndex));
    }

    @Override
    public SQLXML getSQLXML(String parameterName) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "SQLXML", SQLXML.class, List.of(parameterName));
    }

    @Override
    public String getNString(int parameterIndex) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "NString", String.class, List.of(parameterIndex));
    }

    @Override
    public String getNString(String parameterName) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "NString", String.class, List.of(parameterName));
    }

    @Override
    public Reader getNCharacterStream(int parameterIndex) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "NCharacterStream", Reader.class, List.of(parameterIndex));
    }

    @Override
    public Reader getNCharacterStream(String parameterName) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "NCharacterStream", Reader.class, List.of(parameterName));
    }

    @Override
    public Reader getCharacterStream(int parameterIndex) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "CharacterStream", Reader.class, List.of(parameterIndex));
    }

    @Override
    public Reader getCharacterStream(String parameterName) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "CharacterStream", Reader.class, List.of(parameterName));
    }

    @Override
    public void setBlob(String parameterName, Blob x) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Blob", Void.class, List.of(parameterName, x));
    }

    @Override
    public void setClob(String parameterName, Clob x) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Clob", Void.class, List.of(parameterName, x));
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
        this.callProxy(CallType.CALL_SET, "AsciiStream", Void.class, List.of(parameterName, x, length));
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException {
        this.callProxy(CallType.CALL_SET, "BinaryStream", Void.class, List.of(parameterName, x, length));
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
        this.callProxy(CallType.CALL_SET, "CharacterStream", Void.class, List.of(parameterName, reader, length));
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
        this.callProxy(CallType.CALL_SET, "AsciiStream", Void.class, List.of(parameterName, x));
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
        this.callProxy(CallType.CALL_SET, "BinaryStream", Void.class, List.of(parameterName, x));
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
        this.callProxy(CallType.CALL_SET, "CharacterStream", Void.class, List.of(parameterName, reader));
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
        this.callProxy(CallType.CALL_SET, "NCharacterStream", Void.class, List.of(parameterName, value));
    }

    @Override
    public void setClob(String parameterName, Reader reader) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Clob", Void.class, List.of(parameterName, reader));
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Blob", Void.class, List.of(parameterName, inputStream));
    }

    @Override
    public void setNClob(String parameterName, Reader reader) throws SQLException {
        this.callProxy(CallType.CALL_SET, "NClob", Void.class, List.of(parameterName, reader));
    }

    @Override
    public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Object", type, List.of(parameterIndex, type));
    }

    @Override
    public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Object", type, List.of(parameterName, type));
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        String resultSetUUID = this.callProxy(CallType.CALL_EXECUTE, "Query", String.class);
        return new RemoteProxyResultSet(resultSetUUID, this.statementService, this.connection, null);
    }

    @Override
    public int executeUpdate() throws SQLException {
        return this.callProxy(CallType.CALL_EXECUTE, "Update", Integer.class);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Null", Void.class, List.of(parameterIndex, sqlType));
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Boolean", Void.class, List.of(parameterIndex, x));
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Byte", Void.class, List.of(parameterIndex, x));
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Short", Void.class, List.of(parameterIndex, x));
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Int", Void.class, List.of(parameterIndex, x));
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Long", Void.class, List.of(parameterIndex, x));
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Float", Void.class, List.of(parameterIndex, x));
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Double", Void.class, List.of(parameterIndex, x));
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        this.callProxy(CallType.CALL_SET, "BigDecimal", Void.class, List.of(parameterIndex, x));
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        this.callProxy(CallType.CALL_SET, "String", Void.class, List.of(parameterIndex, x));
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Bytes", Void.class, List.of(parameterIndex, x));
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Date", Void.class, List.of(parameterIndex, x));
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Time", Void.class, List.of(parameterIndex, x));
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Timestamp", Void.class, List.of(parameterIndex, x));
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        this.callProxy(CallType.CALL_SET, "AsciiStream", Void.class, List.of(parameterIndex, x, length));
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        this.callProxy(CallType.CALL_SET, "UnicodeStream", Void.class, List.of(parameterIndex, x, length));
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        this.callProxy(CallType.CALL_SET, "BinaryStream", Void.class, List.of(parameterIndex, x, length));
    }

    @Override
    public void clearParameters() throws SQLException {
        this.callProxy(CallType.CALL_CLEAR, "Parameters", Void.class);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Object", Void.class, List.of(parameterIndex, x, targetSqlType));
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Object", Void.class, List.of(parameterIndex, x));
    }

    @Override
    public boolean execute() throws SQLException {
        return this.callProxy(CallType.CALL_EXECUTE, "", Boolean.class);
    }

    @Override
    public void addBatch() throws SQLException {
        this.callProxy(CallType.CALL_ADD, "Batch", Void.class);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        this.callProxy(CallType.CALL_SET, "CharacterStream", Void.class, List.of(parameterIndex, reader, length));
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Ref", Void.class, List.of(parameterIndex, x));
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Blob", Void.class, List.of(parameterIndex, x));
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Clob", Void.class, List.of(parameterIndex, x));
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Array", Void.class, List.of(parameterIndex, x));
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return this.callProxy(CallType.CALL_GET, "MetaData", ResultSetMetaData.class);
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Date", Void.class, List.of(parameterIndex, x, cal));
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Time", Void.class, List.of(parameterIndex, x, cal));
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Timestamp", Void.class, List.of(parameterIndex, x, cal));
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Null", Void.class, List.of(parameterIndex, sqlType, typeName));
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        this.callProxy(CallType.CALL_SET, "URL", Void.class, List.of(parameterIndex, x));
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return this.callProxy(CallType.CALL_GET, "ParameterMetaData", ParameterMetaData.class);
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        this.callProxy(CallType.CALL_SET, "RowId", Void.class, List.of(parameterIndex, x));
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        this.callProxy(CallType.CALL_SET, "NString", Void.class, List.of(parameterIndex, value));
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        this.callProxy(CallType.CALL_SET, "NCharacterStream", Void.class, List.of(parameterIndex, value, length));
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        this.callProxy(CallType.CALL_SET, "NClob", Void.class, List.of(parameterIndex, value));
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Clob", Void.class, List.of(parameterIndex, reader, length));
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Blob", Void.class, List.of(parameterIndex, inputStream, length));
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        this.callProxy(CallType.CALL_SET, "NClob", Void.class, List.of(parameterIndex, reader, length));
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        this.callProxy(CallType.CALL_SET, "SQLXML", Void.class, List.of(parameterIndex, xmlObject));
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Object", Void.class, List.of(parameterIndex, x, targetSqlType, scaleOrLength));
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        this.callProxy(CallType.CALL_SET, "AsciiStream", Void.class, List.of(parameterIndex, x, length));
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        this.callProxy(CallType.CALL_SET, "BinaryStream", Void.class, List.of(parameterIndex, x, length));
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        this.callProxy(CallType.CALL_SET, "CharacterStream", Void.class, List.of(parameterIndex, reader, length));
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        this.callProxy(CallType.CALL_SET, "AsciiStream", Void.class, List.of(parameterIndex, x));
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        this.callProxy(CallType.CALL_SET, "BinaryStream", Void.class, List.of(parameterIndex, x));
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        this.callProxy(CallType.CALL_SET, "CharacterStream", Void.class, List.of(parameterIndex, reader));
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        this.callProxy(CallType.CALL_SET, "NCharacterStream", Void.class, List.of(parameterIndex, value));
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Clob", Void.class, List.of(parameterIndex, reader));
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Blob", Void.class, List.of(parameterIndex, inputStream));
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        this.callProxy(CallType.CALL_SET, "NClob", Void.class, List.of(parameterIndex, reader));
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        return this.callProxy(CallType.CALL_EXECUTE, "Query", ResultSet.class, List.of(sql));
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        return this.callProxy(CallType.CALL_EXECUTE, "Update", Integer.class, List.of(sql));
    }

    @Override
    public void close() throws SQLException {
        this.callProxy(CallType.CALL_CLOSE, "", Void.class);
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return this.callProxy(CallType.CALL_GET, "MaxFieldSize", Integer.class);
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        this.callProxy(CallType.CALL_SET, "MaxFieldSize", Void.class, List.of(max));
    }

    @Override
    public int getMaxRows() throws SQLException {
        return this.callProxy(CallType.CALL_GET, "MaxRows", Integer.class);
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        this.callProxy(CallType.CALL_SET, "MaxRows", Void.class, List.of(max));
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        this.callProxy(CallType.CALL_SET, "EscapeProcessing", Void.class, List.of(enable));
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return this.callProxy(CallType.CALL_GET, "QueryTimeout", Integer.class);
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        this.callProxy(CallType.CALL_SET, "QueryTimeout", Void.class, List.of(seconds));
    }

    @Override
    public void cancel() throws SQLException {
        this.callProxy(CallType.CALL_CANCEL, "", Void.class);
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Warnings", SQLWarning.class);
    }

    @Override
    public void clearWarnings() throws SQLException {
        this.callProxy(CallType.CALL_CLEAR, "Warnings", Void.class);
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        this.callProxy(CallType.CALL_SET, "CursorName", Void.class, List.of(name));
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        return this.callProxy(CallType.CALL_EXECUTE, "", Boolean.class, List.of(sql));
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return this.callProxy(CallType.CALL_GET, "ResultSet", ResultSet.class);
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return this.callProxy(CallType.CALL_GET, "UpdateCount", Integer.class);
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return this.callProxy(CallType.CALL_GET, "MoreResults", Boolean.class);
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        this.callProxy(CallType.CALL_SET, "FetchDirection", Void.class, List.of(direction));
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return this.callProxy(CallType.CALL_GET, "FetchDirection", Integer.class);
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        this.callProxy(CallType.CALL_SET, "FetchSize", Void.class, List.of(rows));
    }

    @Override
    public int getFetchSize() throws SQLException {
        return this.callProxy(CallType.CALL_GET, "FetchSize", Integer.class);
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return this.callProxy(CallType.CALL_GET, "ResultSetConcurrency", Integer.class);
    }

    @Override
    public int getResultSetType() throws SQLException {
        return this.callProxy(CallType.CALL_GET, "ResultSetType", Integer.class);
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        this.callProxy(CallType.CALL_ADD, "Batch", Void.class, List.of(sql));
    }

    @Override
    public void clearBatch() throws SQLException {
        this.callProxy(CallType.CALL_CLEAR, "Batch", Void.class);
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return this.callProxy(CallType.CALL_EXECUTE, "Batch", int[].class);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return this.connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "MoreResults", Boolean.class, List.of(current));
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return this.callProxy(CallType.CALL_GET, "GeneratedKeys", ResultSet.class);
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return this.callProxy(CallType.CALL_EXECUTE, "Update", Integer.class, List.of(sql, autoGeneratedKeys));
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return this.callProxy(CallType.CALL_EXECUTE, "Update", Integer.class, List.of(sql, columnIndexes));
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return this.callProxy(CallType.CALL_EXECUTE, "Update", Integer.class, List.of(sql, columnNames));
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return this.callProxy(CallType.CALL_EXECUTE, "", Boolean.class, List.of(sql, autoGeneratedKeys));
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return this.callProxy(CallType.CALL_EXECUTE, "", Boolean.class, List.of(sql, columnIndexes));
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return this.callProxy(CallType.CALL_EXECUTE, "", Boolean.class, List.of(sql, columnNames));
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return this.callProxy(CallType.CALL_GET, "ResultSetHoldability", Integer.class);
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.callProxy(CallType.CALL_IS, "Closed", Boolean.class);
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        this.callProxy(CallType.CALL_SET, "Poolable", Void.class, List.of(poolable));
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return this.callProxy(CallType.CALL_IS, "Poolable", Boolean.class);
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        this.callProxy(CallType.CALL_CLOSE, "OnCompletion", Void.class);
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return this.callProxy(CallType.CALL_IS, "CloseOnCompletion", Boolean.class);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException("unwrap not supported");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return this.callProxy(CallType.CALL_IS, "WrapperFor", Boolean.class, List.of(iface));
    }

    private CallResourceRequest.Builder newCallBuilder() {
        return CallResourceRequest.newBuilder()
                .setSession(this.connection.getSession())
                .setResourceType(ResourceType.RES_CALLABLE_STATEMENT)
                .setResourceUUID(this.remoteCallableStatementUUID);
    }

    private <T> T callProxy(CallType callType, String targetName, Class returnType) throws SQLException {
        return this.callProxy(callType, targetName, returnType, Constants.EMPTY_OBJECT_LIST);
    }

    private <T> T callProxy(CallType callType, String targetName, Class returnType, List<Object> params) throws SQLException {
        CallResourceRequest.Builder reqBuilder = this.newCallBuilder();
        reqBuilder.setTarget(
                TargetCall.newBuilder()
                        .setCallType(callType)
                        .setResourceName(targetName)
                        .setParams(ByteString.copyFrom(serialize(params)))
                        .build()
        );
        try {
            CallResourceResponse response = this.statementService.callResource(reqBuilder.build());
            this.connection.setSession(response.getSession());
            if (Void.class.equals(returnType)) {
                return null;
            }
            return (T) deserialize(response.getValues().toByteArray(), returnType);
        } catch (StatusRuntimeException sre) {
            throw GrpcExceptionHandler.handle(sre);
        }
    }
}