package org.openjdbcproxy.jdbc;

import com.google.protobuf.ByteString;
import com.openjdbcproxy.grpc.CallResourceRequest;
import com.openjdbcproxy.grpc.CallResourceResponse;
import com.openjdbcproxy.grpc.CallType;
import com.openjdbcproxy.grpc.LobReference;
import com.openjdbcproxy.grpc.LobType;
import com.openjdbcproxy.grpc.ResourceType;
import com.openjdbcproxy.grpc.TargetCall;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.openjdbcproxy.grpc.client.StatementService;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLType;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import static org.openjdbcproxy.grpc.SerializationHandler.deserialize;
import static org.openjdbcproxy.grpc.SerializationHandler.serialize;

/**
 * ResultSet linked to a remote instance of ResultSet in OJP server, it delegates all calls to server instance.
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class RemoteProxyResultSet implements ResultSet {

    private String resultSetUUID;
    private StatementService statementService;
    private Connection connection;
    @Getter
    protected Statement statement;

    public Connection getConnection() throws SQLException {
        if (this.getStatement() != null && this.getStatement().getConnection() != null) {
            return (Connection) this.getStatement().getConnection();
        } else {
            return this.connection;
        }
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException("unwrap not supported.");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLException("unwrap not supported.");
    }

    @Override
    public boolean next() throws SQLException {
        return this.callProxy(CallType.CALL_NEXT, "", Boolean.class);
    }

    @Override
    public void close() throws SQLException {
        this.callProxy(CallType.CALL_CLOSE, "", Void.class);
    }

    @Override
    public boolean wasNull() throws SQLException {
        return this.callProxy(CallType.CALL_WAS, "Null", Boolean.class);
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "String", String.class, List.of(columnIndex));
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Boolean", Boolean.class, List.of(columnIndex));
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Byte", Byte.class, List.of(columnIndex));
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Short", Short.class, List.of(columnIndex));
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Int", Integer.class, List.of(columnIndex));
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Long", Long.class, List.of(columnIndex));
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Float", Float.class, List.of(columnIndex));
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Double", Double.class, List.of(columnIndex));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "BigDecimal", BigDecimal.class, List.of(columnIndex, scale));
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Bytes", byte[].class, List.of(columnIndex));
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Date", Date.class, List.of(columnIndex));
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Time", Time.class, List.of(columnIndex));
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Timestamp", Timestamp.class, List.of(columnIndex));
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "AsciiStream", InputStream.class, List.of(columnIndex));
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "UnicodeStream", InputStream.class, List.of(columnIndex));
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "BinaryStream", InputStream.class, List.of(columnIndex));
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "String", String.class, List.of(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Boolean", Boolean.class, List.of(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Byte", Byte.class, List.of(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Short", Short.class, List.of(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Int", Integer.class, List.of(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Long", Long.class, List.of(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Float", Float.class, List.of(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Double", Double.class, List.of(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "BigDecimal", BigDecimal.class, List.of(columnLabel));
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Bytes", byte[].class, List.of(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Date", Date.class, List.of(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Time", Time.class, List.of(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Timestamp", Timestamp.class, List.of(columnLabel));
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return this.retrieveBinaryStream(CallType.CALL_GET, "AsciiStream", LobType.LT_ASCII_STREAM, List.of(columnLabel));
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return this.retrieveBinaryStream(CallType.CALL_GET, "AsciiStream", LobType.LT_UNICODE_STREAM, List.of(columnLabel));
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return this.retrieveBinaryStream(CallType.CALL_GET, "BinaryStream", LobType.LT_BINARY_STREAM, List.of(columnLabel));
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
    public String getCursorName() throws SQLException {
        return this.callProxy(CallType.CALL_GET, "CursorName", String.class);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new org.openjdbcproxy.jdbc.ResultSetMetaData(this, this.statementService);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Object", Object.class, List.of(columnIndex));
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Object", Object.class, List.of(columnLabel));
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return this.callProxy(CallType.CALL_FIND, "Column", Integer.class, List.of(columnLabel));
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        return this.retrieveReader(CallType.CALL_GET, "CharacterStream", LobType.LT_CHARACTER_STREAM, List.of(columnIndex));
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return this.retrieveReader(CallType.CALL_GET, "CharacterStream", LobType.LT_CHARACTER_STREAM, List.of(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "BigDecimal", BigDecimal.class, List.of(columnIndex));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "BigDecimal", BigDecimal.class, List.of(columnLabel));
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return this.callProxy(CallType.CALL_IS, "BeforeFirst", Boolean.class);
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return this.callProxy(CallType.CALL_IS, "AfterLast", Boolean.class);
    }

    @Override
    public boolean isFirst() throws SQLException {
        return this.callProxy(CallType.CALL_IS, "First", Boolean.class);
    }

    @Override
    public boolean isLast() throws SQLException {
        return this.callProxy(CallType.CALL_IS, "Last", Boolean.class);
    }

    @Override
    public void beforeFirst() throws SQLException {
        this.callProxy(CallType.CALL_BEFORE, "First", Void.class);
    }

    @Override
    public void afterLast() throws SQLException {
        this.callProxy(CallType.CALL_AFTER, "Last", Void.class);
    }

    @Override
    public boolean first() throws SQLException {
        return this.callProxy(CallType.CALL_FIRST, "", Boolean.class);
    }

    @Override
    public boolean last() throws SQLException {
        return this.callProxy(CallType.CALL_LAST, "", Boolean.class);
    }

    @Override
    public int getRow() throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Row", Integer.class);
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        return this.callProxy(CallType.CALL_ABSOLUTE, "", Boolean.class, List.of(row));
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        return this.callProxy(CallType.CALL_RELATIVE, "", Boolean.class, List.of(rows));
    }

    @Override
    public boolean previous() throws SQLException {
        return this.callProxy(CallType.CALL_PREVIOUS, "", Boolean.class);
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
    public int getType() throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Type", Integer.class);
    }

    @Override
    public int getConcurrency() throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Concurrency", Integer.class);
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        return this.callProxy(CallType.CALL_ROW, "Updated", Boolean.class);
    }

    @Override
    public boolean rowInserted() throws SQLException {
        return this.callProxy(CallType.CALL_ROW, "Inserted", Boolean.class);
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        return this.callProxy(CallType.CALL_ROW, "Deleted", Boolean.class);
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        this.callProxy(CallType.CALL_UPDATE, "Null", Void.class, List.of(columnIndex));
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        this.callProxy(CallType.CALL_UPDATE, "Boolean", Void.class, List.of(columnIndex, x));
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        this.callProxy(CallType.CALL_UPDATE, "Byte", Void.class, List.of(columnIndex, x));
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        this.callProxy(CallType.CALL_UPDATE, "Byte", Void.class, List.of(columnIndex, x));
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        this.callProxy(CallType.CALL_UPDATE, "Int", Void.class, List.of(columnIndex, x));
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        this.callProxy(CallType.CALL_UPDATE, "Long", Void.class, List.of(columnIndex, x));
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        this.callProxy(CallType.CALL_UPDATE, "Float", Void.class, List.of(columnIndex, x));
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        this.callProxy(CallType.CALL_UPDATE, "Double", Void.class, List.of(columnIndex, x));
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        this.callProxy(CallType.CALL_UPDATE, "BigDecimal", Void.class, List.of(columnIndex, x));
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        this.callProxy(CallType.CALL_UPDATE, "String", Void.class, List.of(columnIndex, x));
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        this.callProxy(CallType.CALL_UPDATE, "Bytes", Void.class, List.of(columnIndex, x));
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        this.callProxy(CallType.CALL_UPDATE, "Date", Void.class, List.of(columnIndex, x));
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        this.callProxy(CallType.CALL_UPDATE, "Time", Void.class, List.of(columnIndex, x));
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        this.callProxy(CallType.CALL_UPDATE, "Timestamp", Void.class, List.of(columnIndex, x));
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        this.callProxy(CallType.CALL_UPDATE, "Object", Void.class, List.of(columnIndex, x, scaleOrLength));
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        this.callProxy(CallType.CALL_UPDATE, "Object", Void.class, List.of(columnIndex, x));
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        this.callProxy(CallType.CALL_UPDATE, "Null", Void.class, List.of(columnLabel));
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        this.callProxy(CallType.CALL_UPDATE, "Boolean", Void.class, List.of(columnLabel, x));
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        this.callProxy(CallType.CALL_UPDATE, "Byte", Void.class, List.of(columnLabel, x));
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        this.callProxy(CallType.CALL_UPDATE, "Short", Void.class, List.of(columnLabel, x));
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        this.callProxy(CallType.CALL_UPDATE, "Int", Void.class, List.of(columnLabel, x));
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        this.callProxy(CallType.CALL_UPDATE, "Long", Void.class, List.of(columnLabel, x));
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        this.callProxy(CallType.CALL_UPDATE, "Float", Void.class, List.of(columnLabel, x));
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        this.callProxy(CallType.CALL_UPDATE, "Double", Void.class, List.of(columnLabel, x));
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        this.callProxy(CallType.CALL_UPDATE, "BigDecimal", Void.class, List.of(columnLabel, x));
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        this.callProxy(CallType.CALL_UPDATE, "String", Void.class, List.of(columnLabel, x));
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        this.callProxy(CallType.CALL_UPDATE, "Bytes", Void.class, List.of(columnLabel, x));
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        this.callProxy(CallType.CALL_UPDATE, "Date", Void.class, List.of(columnLabel, x));
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        this.callProxy(CallType.CALL_UPDATE, "Time", Void.class, List.of(columnLabel, x));
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        this.callProxy(CallType.CALL_UPDATE, "Timestamp", Void.class, List.of(columnLabel, x));
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        this.callProxy(CallType.CALL_UPDATE, "Object", Void.class, List.of(columnLabel, x, scaleOrLength));
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        this.callProxy(CallType.CALL_UPDATE, "Object", Void.class, List.of(columnLabel, x));
    }

    @Override
    public void insertRow() throws SQLException {
        this.callProxy(CallType.CALL_INSERT, "Row", Void.class);
    }

    @Override
    public void updateRow() throws SQLException {
        this.callProxy(CallType.CALL_UPDATE, "Row", Void.class);
    }

    @Override
    public void deleteRow() throws SQLException {
        this.callProxy(CallType.CALL_DELETE, "Row", Void.class);
    }

    @Override
    public void refreshRow() throws SQLException {
        this.callProxy(CallType.CALL_REFRESH, "Row", Void.class);
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        this.callProxy(CallType.CALL_CANCEL, "Row", Void.class);
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        this.callProxy(CallType.CALL_MOVE, "ToInsertRow", Void.class);
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        this.callProxy(CallType.CALL_MOVE, "ToCurrentRow", Void.class);
    }

    @Override
    public Statement getStatement() throws SQLException {
        return this.statement;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Object", Object.class, List.of(columnIndex, map));
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Object", Object.class, List.of(columnLabel, map));
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Date", Date.class, List.of(columnIndex, cal));
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Date", Date.class, List.of(columnLabel, cal));
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Time", Date.class, List.of(columnIndex, cal));
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Time", Date.class, List.of(columnLabel, cal));
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Timestamp", Timestamp.class, List.of(columnIndex, cal));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Timestamp", Timestamp.class, List.of(columnLabel, cal));
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "URL", Timestamp.class, List.of(columnIndex));
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "URL", Timestamp.class, List.of(columnLabel));
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        this.callProxy(CallType.CALL_UPDATE, "RowId", Void.class, List.of(columnIndex, x));
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        this.callProxy(CallType.CALL_UPDATE, "RowId", Void.class, List.of(columnLabel, x));
    }

    @Override
    public int getHoldability() throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Holdability", Integer.class);
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.callProxy(CallType.CALL_IS, "Closed", Boolean.class);
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        this.callProxy(CallType.CALL_UPDATE, "NString", Void.class, List.of(columnIndex, nString));
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        this.callProxy(CallType.CALL_UPDATE, "NString", Void.class, List.of(columnLabel, nString));
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "NString", String.class, List.of(columnIndex));
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "NString", String.class, List.of(columnLabel));
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return this.retrieveReader(CallType.CALL_GET, "NCharacterStream", LobType.LT_CHARACTER_STREAM, List.of(columnIndex));
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return this.retrieveReader(CallType.CALL_GET, "NCharacterStream", LobType.LT_CHARACTER_STREAM, List.of(columnLabel));
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Object", type, List.of(columnIndex, type));
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return this.callProxy(CallType.CALL_GET, "Object", type, List.of(columnLabel, type));
    }

    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        ResultSet.super.updateObject(columnIndex, x, targetSqlType, scaleOrLength);
    }

    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        ResultSet.super.updateObject(columnLabel, x, targetSqlType, scaleOrLength);
    }

    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType) throws SQLException {
        ResultSet.super.updateObject(columnIndex, x, targetSqlType);
    }

    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType) throws SQLException {
        ResultSet.super.updateObject(columnLabel, x, targetSqlType);
    }

    private CallResourceRequest.Builder newCallBuilder() throws SQLException {
        return CallResourceRequest.newBuilder()
                .setSession(this.getConnection().getSession())
                .setResourceType(ResourceType.RES_RESULT_SET)
                .setResourceUUID(this.resultSetUUID);
    }

    private <T> T callProxy(CallType callType, String target, Class returnType) throws SQLException {
        return this.callProxy(callType, target, returnType, Constants.EMPTY_OBJECT_LIST);
    }

    /**
     * Calls a method or attribute in the remote OJP proxy server.
     *
     * @param callType   - Call type prefix, for example GET, SET, UPDATE...
     * @param target     - Target name of the method or attribute being called.
     * @param returnType - Type returned if a return is present, if not Void.class
     * @param params     - List of parameters required to execute the method.
     * @return - Returns the type passed as returnType parameter.
     * @throws SQLException - In case of failure of call or interface not supported.
     */
    private <T> T callProxy(CallType callType, String target, Class returnType, List<Object> params) throws SQLException {
        CallResourceRequest.Builder reqBuilder = this.newCallBuilder();
        reqBuilder.setTarget(
                        TargetCall.newBuilder()
                                .setCallType(callType)
                                .setResourceName(target)
                                .setParams(ByteString.copyFrom(serialize(params)))
                                .build()
                );
        CallResourceResponse response = this.statementService.callResource(reqBuilder.build());
        this.getConnection().setSession(response.getSession());
        if (Void.class.equals(returnType)) {
            return null;
        }
        return (T) deserialize(response.getValues().toByteArray(), returnType);
    }

    private Reader retrieveReader(CallType callType, String attrName, LobType lobType, List<Object> params)
            throws SQLException {
        return new InputStreamReader(this.retrieveBinaryStream(callType, attrName, lobType, params));
    }

    /**
     * Retrieves an attribute from the linked remote instance of the ResultSet.
     *
     * @param callType - Call type prefix, for example GET.
     * @param attrName - Name of the target attribute.
     * @param lobType  - Type of the LOB being retrieved LT_BINARY_STREAM.
     * @param params   - List of parameters required to execute the method.
     * @return InputStream - resulting input stream.
     * @throws SQLException
     */
    private InputStream retrieveBinaryStream(CallType callType, String attrName, LobType lobType, List<Object> params)
            throws SQLException {
        CallResourceRequest.Builder reqBuilder = this.newCallBuilder();
        reqBuilder.setTarget(
                TargetCall.newBuilder()
                        .setCallType(callType)
                        .setResourceName(attrName)
                        .setParams(ByteString.copyFrom(serialize(params)))
                        .build()
        );
        CallResourceResponse response = this.statementService.callResource(reqBuilder.build());
        this.getConnection().setSession(response.getSession());
        String lobRefUUID = deserialize(response.getValues().toByteArray(), String.class);
        BinaryStream binaryStream = new BinaryStream(
                this.presentConnection(),
                new LobServiceImpl(this.presentConnection(), this.getStatementService()),
                this.getStatementService(),
                LobReference.newBuilder()
                        .setSession(this.getConnection().getSession())
                        .setLobType(lobType)
                        .setUuid(lobRefUUID)
                        .build());
        return binaryStream.getBinaryStream();
    }

    private Connection presentConnection() throws SQLException {
        //Statement is not guaranteed to be present, for example when the result set is created from the DatabaseMetaData.
        if (this.statement != null && this.statement.getConnection() != null) {
            return (Connection) this.statement.getConnection();
        }
        return this.getConnection();
    }
}
