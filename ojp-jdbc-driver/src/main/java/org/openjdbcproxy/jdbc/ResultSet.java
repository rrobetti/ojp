package org.openjdbcproxy.jdbc;

import com.openjdbcproxy.grpc.LobReference;
import com.openjdbcproxy.grpc.LobType;
import com.openjdbcproxy.grpc.OpResult;
import io.grpc.StatusRuntimeException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.openjdbcproxy.constants.CommonConstants;
import org.openjdbcproxy.grpc.client.StatementService;
import org.openjdbcproxy.grpc.dto.OpQueryResult;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.openjdbcproxy.grpc.SerializationHandler.deserialize;
import static org.openjdbcproxy.grpc.client.GrpcExceptionHandler.handle;

@Slf4j
public class ResultSet extends RemoteProxyResultSet {

    @Getter
    private final Map<String, Integer> labelsMap;

    private Iterator<OpResult> itResults;
    private List<Object[]> currentDataBlock;
    private AtomicInteger blockIdx = new AtomicInteger(-1);
    private AtomicInteger blockCount = new AtomicInteger(1);
    @Getter
    private java.sql.Statement statement;
    private java.sql.ResultSetMetaData resultSetMetadata;

    public ResultSet(Iterator<OpResult> itOpResult, StatementService statementService, java.sql.Statement statement) throws SQLException {
        this.itResults = itOpResult;
        try {
            this.statement = statement;
            OpResult result = nextWithSessionUpdate(itOpResult.next());
            OpQueryResult opQueryResult = deserialize(result.getValue().toByteArray(), OpQueryResult.class);

            this.setStatementService(statementService);
            this.setResultSetUUID(opQueryResult.getResultSetUUID());
            this.currentDataBlock = opQueryResult.getRows();
            this.labelsMap = new HashMap<>();
            List<String> labels = opQueryResult.getLabels();
            for (int i = 0; i < labels.size(); i++) {
                labelsMap.put(labels.get(i).toUpperCase(), i);
            }
        } catch (StatusRuntimeException e) {
            throw handle(e);
        }
    }

    @Override
    public boolean next() throws SQLException {
        blockIdx.incrementAndGet();
        if (blockIdx.get() >= currentDataBlock.size() && itResults.hasNext()) {
            try {
                OpResult result = this.nextWithSessionUpdate(itResults.next());
                OpQueryResult opQueryResult = deserialize(result.getValue().toByteArray(), OpQueryResult.class);
                this.currentDataBlock = opQueryResult.getRows();
                this.blockCount.incrementAndGet();
                this.blockIdx.set(0);
            } catch (StatusRuntimeException e) {
                throw handle(e);
            }
        }

        return blockIdx.get() < currentDataBlock.size();
    }

    private OpResult nextWithSessionUpdate(OpResult next) throws SQLException {
        ((Connection)this.statement.getConnection()).setSession(next.getSession());
        return next;
    }

    @Override
    public void close() throws SQLException {
        this.blockIdx = null;
        this.currentDataBlock = null;
    }

    @Override
    public boolean wasNull() throws SQLException {
        return false;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        return (String) currentDataBlock.get(blockIdx.get())[columnIndex -1];
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return (boolean) currentDataBlock.get(blockIdx.get())[columnIndex -1];
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return (byte) currentDataBlock.get(blockIdx.get())[columnIndex -1];
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return (short) currentDataBlock.get(blockIdx.get())[columnIndex -1];
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        Object value = currentDataBlock.get(blockIdx.get())[columnIndex - 1];
        if (value instanceof Long lValue) {
            return lValue.intValue();
        } else {
            return (int) value;
        }
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return (long) currentDataBlock.get(blockIdx.get())[columnIndex -1];
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        Object value = currentDataBlock.get(blockIdx.get())[columnIndex -1];
        if (value instanceof BigDecimal bdValue) {
            return bdValue.floatValue();
        }
        return (float) value;
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        Object value = currentDataBlock.get(blockIdx.get())[columnIndex -1];
        if (value instanceof BigDecimal bdValue) {
            return bdValue.doubleValue();
        }
        return (double) value;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return (BigDecimal) currentDataBlock.get(blockIdx.get())[columnIndex -1];
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        return (byte[]) currentDataBlock.get(blockIdx.get())[columnIndex -1];
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return (Date) currentDataBlock.get(blockIdx.get())[columnIndex -1];
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return (Time) currentDataBlock.get(blockIdx.get())[columnIndex -1];
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return (Timestamp) currentDataBlock.get(blockIdx.get())[columnIndex -1];
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        Object objUUID = currentDataBlock.get(blockIdx.get())[columnIndex -1];
        String lobRefUUID = String.valueOf(objUUID);
        BinaryStream binaryStream = new BinaryStream((Connection) this.statement.getConnection(),
                new LobServiceImpl((Connection) this.statement.getConnection(), this.getStatementService()),
                this.getStatementService(),
                LobReference.newBuilder()
                        .setSession(this.getConnection().getSession())
                        .setLobType(LobType.LT_BINARY_STREAM)
                        .setUuid(lobRefUUID)
                        .setColumnIndex(columnIndex)
                        .build());
        return binaryStream.getBinaryStream();
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return (String) currentDataBlock.get(blockIdx.get())[this.labelsMap.get(columnLabel.toUpperCase())];
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return (boolean) currentDataBlock.get(blockIdx.get())[this.labelsMap.get(columnLabel.toUpperCase())];
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return (byte) currentDataBlock.get(blockIdx.get())[this.labelsMap.get(columnLabel.toUpperCase())];
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return (short) currentDataBlock.get(blockIdx.get())[this.labelsMap.get(columnLabel.toUpperCase())];
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return this.getInt(this.labelsMap.get(columnLabel.toUpperCase()) + 1);
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return this.getLong(this.labelsMap.get(columnLabel.toUpperCase()) + 1);
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return this.getFloat(this.labelsMap.get(columnLabel.toUpperCase()) + 1);
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return this.getDouble(this.labelsMap.get(columnLabel.toUpperCase()) + 1);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return (BigDecimal) currentDataBlock.get(blockIdx.get())[this.labelsMap.get(columnLabel.toUpperCase())];
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return (byte[]) currentDataBlock.get(blockIdx.get())[this.labelsMap.get(columnLabel.toUpperCase())];
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return (Date) currentDataBlock.get(blockIdx.get())[this.labelsMap.get(columnLabel.toUpperCase())];
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return (Time) currentDataBlock.get(blockIdx.get())[this.labelsMap.get(columnLabel.toUpperCase())];
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return (Timestamp) currentDataBlock.get(blockIdx.get())[this.labelsMap.get(columnLabel.toUpperCase())];
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return this.getBinaryStream(this.labelsMap.get(columnLabel.toUpperCase()) + 1);
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null; //TODO to be implemented in a single call to server which will use reflection to apply
    }

    @Override
    public void clearWarnings() throws SQLException {
        //TODO to be implemented in a single call to server which will use reflection to apply
    }

    @Override
    public String getCursorName() throws SQLException {
        return ""; //TODO to be implemented in a single call to server which will use reflection to apply
    }

    @Override
    public java.sql.ResultSetMetaData getMetaData() throws SQLException {
        if (this.getResultSetUUID() == null) {
            throw new SQLException("No result set reference found.");
        }
        if (this.resultSetMetadata == null) {
            this.resultSetMetadata = new ResultSetMetaData(this, this.getStatementService());
        }
        return this.resultSetMetadata;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return currentDataBlock.get(blockIdx.get())[columnIndex -1];
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return currentDataBlock.get(blockIdx.get())[this.labelsMap.get(columnLabel.toUpperCase())];
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return this.labelsMap.get(columnLabel);
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return (BigDecimal) currentDataBlock.get(blockIdx.get())[columnIndex -1];
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return (BigDecimal) currentDataBlock.get(blockIdx.get())[this.labelsMap.get(columnLabel.toUpperCase())];
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return blockIdx.get() == -1;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return !itResults.hasNext() && blockIdx.get() >= currentDataBlock.size();
    }

    @Override
    public synchronized boolean isFirst() throws SQLException {
        return this.blockCount.get() == 1 && this.blockIdx.get() == 0;
    }

    @Override
    public boolean isLast() throws SQLException {
        return !itResults.hasNext() && blockIdx.get() == (currentDataBlock.size() - 1);
    }

    @Override
    public void beforeFirst() throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void afterLast() throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public boolean first() throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public boolean last() throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public int getRow() throws SQLException {
        return ((this.blockCount.get() - 1) * CommonConstants.ROWS_PER_RESULT_SET_DATA_BLOCK) + this.blockIdx.get() + 1;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public boolean previous() throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public int getFetchDirection() throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public int getFetchSize() throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public int getType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;//Only type supported at the moment.
    }

    @Override
    public int getConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public boolean rowInserted() throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void insertRow() throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateRow() throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void deleteRow() throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void refreshRow() throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public java.sql.Statement getStatement() throws SQLException {
        return this.statement;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        Object objUUID = currentDataBlock.get(blockIdx.get())[columnIndex -1];
        String blobRefUUID = String.valueOf(objUUID);
        return new org.openjdbcproxy.jdbc.Blob((Connection) this.statement.getConnection(),
                new LobServiceImpl((Connection) this.statement.getConnection(), this.getStatementService()),
                this.getStatementService(),
                LobReference.newBuilder()
                        .setSession(((Connection) this.statement.getConnection()).getSession())
                        .setUuid(blobRefUUID)
                        .build()
        );
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        String clobRefUUID = (String) currentDataBlock.get(blockIdx.get())[columnIndex -1];
        return new org.openjdbcproxy.jdbc.Clob((Connection) this.statement.getConnection(),
                new LobServiceImpl((Connection) this.statement.getConnection(), this.getStatementService()),
                this.getStatementService(),
                LobReference.newBuilder()
                        .setSession(((Connection) this.statement.getConnection()).getSession())
                        .setUuid(clobRefUUID)
                        .setLobType(LobType.LT_CLOB)
                        .build()
        );
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        String blobRefUUID = (String) currentDataBlock.get(blockIdx.get())[this.labelsMap.get(columnLabel.toUpperCase())];
        return new org.openjdbcproxy.jdbc.Blob((Connection) this.statement.getConnection(),
                new LobServiceImpl((Connection) this.statement.getConnection(), this.getStatementService()),
                this.getStatementService(),
                LobReference.newBuilder()
                        .setSession(((Connection) this.statement.getConnection()).getSession())
                        .setUuid(blobRefUUID)
                        .build()
        );
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        return (URL) currentDataBlock.get(blockIdx.get())[columnIndex -1];
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return (URL) currentDataBlock.get(blockIdx.get())[this.labelsMap.get(columnLabel.toUpperCase())];
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public int getHoldability() throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public boolean isClosed() throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return (T) currentDataBlock.get(blockIdx.get())[columnIndex -1];
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return (T) currentDataBlock.get(blockIdx.get())[this.labelsMap.get(columnLabel.toUpperCase())];
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new RuntimeException("Not implemented");
    }
}
