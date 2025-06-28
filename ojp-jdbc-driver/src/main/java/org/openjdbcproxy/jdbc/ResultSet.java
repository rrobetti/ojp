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

    private Iterator<OpResult> itResults;//Iterator of blocks of data
    private List<Object[]> currentDataBlock;//Current block of data being processed.
    private AtomicInteger blockIdx = new AtomicInteger(-1);//Current block index
    private AtomicInteger blockCount = new AtomicInteger(1);//Current block count
    private java.sql.ResultSetMetaData resultSetMetadata;
    private boolean inProxyMode;
    private boolean closed;
    private AtomicInteger currentIdx = new AtomicInteger(0);

    private Object lastValueRead;

    public ResultSet(Iterator<OpResult> itOpResult, StatementService statementService, java.sql.Statement statement) throws SQLException {
        this.itResults = itOpResult;
        this.inProxyMode = false;
        this.closed = false;
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
        if (this.inProxyMode) {
            return super.next();
        }
        this.currentIdx.incrementAndGet();
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
        this.closed = true;
        this.blockIdx = null;
        this.itResults = null;
        this.currentDataBlock = null;
        super.close();
    }

    @Override
    public boolean wasNull() throws SQLException {
        if (this.inProxyMode) {
            return super.wasNull();
        }
        return lastValueRead == null;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        if (this.inProxyMode) {
            return super.getString(columnIndex);
        }
        lastValueRead = currentDataBlock.get(blockIdx.get())[columnIndex -1];
        return (String) lastValueRead;
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        if (this.inProxyMode) {
            return super.getBoolean(columnIndex);
        }
        lastValueRead = currentDataBlock.get(blockIdx.get())[columnIndex -1];
        return (boolean) lastValueRead;
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        if (this.inProxyMode) {
            return super.getByte(columnIndex);
        }
        lastValueRead = currentDataBlock.get(blockIdx.get())[columnIndex -1];
        return (byte) lastValueRead;
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        if (this.inProxyMode) {
            return super.getShort(columnIndex);
        }
        lastValueRead = currentDataBlock.get(blockIdx.get())[columnIndex -1];
        return (short) lastValueRead;
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        if (this.inProxyMode) {
            return super.getInt(columnIndex);
        }
        lastValueRead = currentDataBlock.get(blockIdx.get())[columnIndex - 1];
        Object value = lastValueRead;
        if (value instanceof Long lValue) {
            return lValue.intValue();
        } else {
            return (int) value;
        }
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        if (this.inProxyMode) {
            return super.getLong(columnIndex);
        }
        lastValueRead = currentDataBlock.get(blockIdx.get())[columnIndex -1];
        return (long) lastValueRead;
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        if (this.inProxyMode) {
            return super.getFloat(columnIndex);
        }
        lastValueRead = currentDataBlock.get(blockIdx.get())[columnIndex -1];
        Object value = lastValueRead;
        if (value instanceof BigDecimal bdValue) {
            return bdValue.floatValue();
        }
        return (float) value;
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        if (this.inProxyMode) {
            return super.getDouble(columnIndex);
        }
        lastValueRead = currentDataBlock.get(blockIdx.get())[columnIndex -1];
        Object value = lastValueRead;
        if (value instanceof BigDecimal bdValue) {
            return bdValue.doubleValue();
        }
        return (double) value;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        if (this.inProxyMode) {
            return super.getBigDecimal(columnIndex, scale);
        }
        lastValueRead = currentDataBlock.get(blockIdx.get())[columnIndex -1];
        return (BigDecimal) lastValueRead;
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        if (this.inProxyMode) {
            return super.getBytes(columnIndex);
        }
        lastValueRead = currentDataBlock.get(blockIdx.get())[columnIndex -1];
        return (byte[]) lastValueRead;
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        if (this.inProxyMode) {
            return super.getDate(columnIndex);
        }
        lastValueRead = currentDataBlock.get(blockIdx.get())[columnIndex -1];
        Object result = lastValueRead;
        if (result instanceof Timestamp timestamp) {
            return new Date(timestamp.getTime());
        }
        return (Date) result;
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        if (this.inProxyMode) {
            return super.getTime(columnIndex);
        }
        lastValueRead = currentDataBlock.get(blockIdx.get())[columnIndex -1];
        return (Time) lastValueRead;
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        if (this.inProxyMode) {
            return super.getTimestamp(columnIndex);
        }
        lastValueRead = currentDataBlock.get(blockIdx.get())[columnIndex -1];
        return (Timestamp) lastValueRead;
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        if (this.inProxyMode) {
            return super.getAsciiStream(columnIndex);
        }
        lastValueRead = null;
        throw new RuntimeException("Not implemented");
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        if (this.inProxyMode) {
            return super.getUnicodeStream(columnIndex);
        }
        lastValueRead = null;
        throw new RuntimeException("Not implemented");
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        if (this.inProxyMode) {
            return super.getBinaryStream(columnIndex);
        }
        lastValueRead = currentDataBlock.get(blockIdx.get())[columnIndex -1];
        Object objUUID = lastValueRead;
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
        if (this.inProxyMode) {
            return super.getString(columnLabel);
        }
        lastValueRead = currentDataBlock.get(blockIdx.get())[this.labelsMap.get(columnLabel.toUpperCase())];
        return (String) lastValueRead;
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        if (this.inProxyMode) {
            return super.getBoolean(columnLabel);
        }
        lastValueRead = currentDataBlock.get(blockIdx.get())[this.labelsMap.get(columnLabel.toUpperCase())];
        return (boolean) lastValueRead;
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        if (this.inProxyMode) {
            return super.getByte(columnLabel);
        }
        lastValueRead = currentDataBlock.get(blockIdx.get())[this.labelsMap.get(columnLabel.toUpperCase())];
        return (byte) lastValueRead;
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        if (this.inProxyMode) {
            return super.getShort(columnLabel);
        }
        lastValueRead = currentDataBlock.get(blockIdx.get())[this.labelsMap.get(columnLabel.toUpperCase())];
        return (short) lastValueRead;
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        if (this.inProxyMode) {
            return super.getInt(columnLabel);
        }
        int colIdx = this.labelsMap.get(columnLabel.toUpperCase()) + 1;
        lastValueRead = currentDataBlock.get(blockIdx.get())[colIdx - 1];
        Object value = lastValueRead;
        if (value instanceof Long lValue) {
            return lValue.intValue();
        } else {
            return (int) value;
        }
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        if (this.inProxyMode) {
            return super.getLong(columnLabel);
        }
        int colIdx = this.labelsMap.get(columnLabel.toUpperCase()) + 1;
        lastValueRead = currentDataBlock.get(blockIdx.get())[colIdx - 1];
        return (long) lastValueRead;
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        if (this.inProxyMode) {
            return super.getFloat(columnLabel);
        }
        int colIdx = this.labelsMap.get(columnLabel.toUpperCase()) + 1;
        lastValueRead = currentDataBlock.get(blockIdx.get())[colIdx - 1];
        Object value = lastValueRead;
        if (value instanceof BigDecimal bdValue) {
            return bdValue.floatValue();
        }
        return (float) value;
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        if (this.inProxyMode) {
            return super.getDouble(columnLabel);
        }
        int colIdx = this.labelsMap.get(columnLabel.toUpperCase()) + 1;
        lastValueRead = currentDataBlock.get(blockIdx.get())[colIdx - 1];
        Object value = lastValueRead;
        if (value instanceof BigDecimal bdValue) {
            return bdValue.doubleValue();
        }
        return (double) value;
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        if (this.inProxyMode) {
            return super.getBigDecimal(columnLabel, scale);
        }
        lastValueRead = currentDataBlock.get(blockIdx.get())[this.labelsMap.get(columnLabel.toUpperCase())];
        return (BigDecimal) lastValueRead;
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        if (this.inProxyMode) {
            return super.getBytes(columnLabel);
        }
        lastValueRead = currentDataBlock.get(blockIdx.get())[this.labelsMap.get(columnLabel.toUpperCase())];
        return (byte[]) lastValueRead;
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        if (this.inProxyMode) {
            return super.getDate(columnLabel);
        }
        int colIdx = this.labelsMap.get(columnLabel.toUpperCase()) + 1;
        lastValueRead = currentDataBlock.get(blockIdx.get())[colIdx - 1];
        Object result = lastValueRead;
        if (result instanceof Timestamp timestamp) {
            return new Date(timestamp.getTime());
        }
        return (Date) result;
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        if (this.inProxyMode) {
            return super.getTime(columnLabel);
        }
        lastValueRead = currentDataBlock.get(blockIdx.get())[this.labelsMap.get(columnLabel.toUpperCase())];
        return (Time) lastValueRead;
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        if (this.inProxyMode) {
            return super.getTimestamp(columnLabel);
        }
        lastValueRead = currentDataBlock.get(blockIdx.get())[this.labelsMap.get(columnLabel.toUpperCase())];
        return (Timestamp) lastValueRead;
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        if (this.inProxyMode) {
            return super.getAsciiStream(columnLabel);
        }
        lastValueRead = null;
        throw new RuntimeException("Not implemented");
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        if (this.inProxyMode) {
            return super.getUnicodeStream(columnLabel);
        }
        lastValueRead = null;
        throw new RuntimeException("Not implemented");
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        if (this.inProxyMode) {
            return super.getBinaryStream(columnLabel);
        }
        int colIdx = this.labelsMap.get(columnLabel.toUpperCase()) + 1;
        lastValueRead = currentDataBlock.get(blockIdx.get())[colIdx - 1];
        return this.getBinaryStream(colIdx);
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return super.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        if (this.inProxyMode) {
            super.clearWarnings();
        }
    }

    @Override
    public String getCursorName() throws SQLException {
        if (this.inProxyMode) {
            return super.getCursorName();
        }
        return "";
    }

    @Override
    public java.sql.ResultSetMetaData getMetaData() throws SQLException {
        if (this.inProxyMode) {
            return super.getMetaData();
        }
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
        if (this.inProxyMode) {
            return super.getObject(columnIndex);
        }
        lastValueRead = currentDataBlock.get(blockIdx.get())[columnIndex -1];
        return lastValueRead;
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        if (this.inProxyMode) {
            return super.getObject(columnLabel);
        }
        lastValueRead = currentDataBlock.get(blockIdx.get())[this.labelsMap.get(columnLabel.toUpperCase())];
        return lastValueRead;
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        if (this.inProxyMode) {
            return super.findColumn(columnLabel);
        }
        return this.labelsMap.get(columnLabel);
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        if (this.inProxyMode) {
            return super.getCharacterStream(columnIndex);
        }
        lastValueRead = null;
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        if (this.inProxyMode) {
            return super.getCharacterStream(columnLabel);
        }
        lastValueRead = null;
        throw new RuntimeException("Not implemented");
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        if (this.inProxyMode) {
            return super.getBigDecimal(columnIndex);
        }
        lastValueRead = currentDataBlock.get(blockIdx.get())[columnIndex -1];
        return (BigDecimal) lastValueRead;
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        if (this.inProxyMode) {
            return super.getBigDecimal(columnLabel);
        }
        lastValueRead = currentDataBlock.get(blockIdx.get())[this.labelsMap.get(columnLabel.toUpperCase())];
        return (BigDecimal) lastValueRead;
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        if (this.inProxyMode) {
            return super.isBeforeFirst();
        }
        return blockIdx.get() == -1;
    }

    @Override
    public boolean first() throws SQLException {
        this.inProxyMode = true;
        return super.first();
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        if (this.inProxyMode) {
            return super.isAfterLast();
        }
        return !itResults.hasNext() && blockIdx.get() >= currentDataBlock.size();
    }

    @Override
    public synchronized boolean isFirst() throws SQLException {
        if (this.inProxyMode) {
            return super.isFirst();
        }
        return this.blockCount.get() == 1 && this.blockIdx.get() == 0;
    }

    @Override
    public boolean isLast() throws SQLException {
        if (this.inProxyMode) {
            return super.isLast();
        }
        return !itResults.hasNext() && blockIdx.get() == (currentDataBlock.size() - 1);
    }

    @Override
    public void beforeFirst() throws SQLException {
        this.inProxyMode = true;
        super.beforeFirst();
    }

    @Override
    public void afterLast() throws SQLException {
        this.inProxyMode = true;
        super.afterLast();
    }

    @Override
    public boolean last() throws SQLException {
        this.inProxyMode = true;
        return super.last();
    }

    @Override
    public int getRow() throws SQLException {
        if (this.inProxyMode) {
            return super.getRow();
        }
        return ((this.blockCount.get() - 1) * CommonConstants.ROWS_PER_RESULT_SET_DATA_BLOCK) + this.blockIdx.get() + 1;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        this.inProxyMode = true;
        return super.absolute(row);
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        this.inProxyMode = true;
        return super.relative(rows);
    }

    @Override
    public boolean previous() throws SQLException {
        if (this.inProxyMode) {
            return super.previous();
        }
        this.inProxyMode = true;
        return super.absolute(this.currentIdx.get() - 1);// Will reposition the cursor in the current row being processed.
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        super.setFetchDirection(direction);
        this.inProxyMode = true;
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return super.getFetchDirection();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        if (this.inProxyMode) {
            super.setFetchSize(rows);
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public int getFetchSize() throws SQLException {
        if (this.inProxyMode) {
            return super.getFetchSize();
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public int getType() throws SQLException {
        if (this.inProxyMode) {
            return super.getType();
        }
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() throws SQLException {
        if (this.inProxyMode) {
            return super.getConcurrency();
        }
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        if (this.inProxyMode) {
            return super.rowUpdated();
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public boolean rowInserted() throws SQLException {
        if (this.inProxyMode) {
            return super.rowInserted();
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        if (this.inProxyMode) {
            return super.rowDeleted();
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        this.inProxyMode = true;
        super.absolute(this.currentIdx.get());
        super.updateNull(columnIndex);
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        this.inProxyMode = true;
        super.absolute(this.currentIdx.get());
        super.updateBoolean(columnIndex, x);
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        if (this.inProxyMode) {
            super.updateByte(columnIndex, x);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        if (this.inProxyMode) {
            super.updateShort(columnIndex, x);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        if (this.inProxyMode) {
            super.updateInt(columnIndex, x);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        if (this.inProxyMode) {
            super.updateLong(columnIndex, x);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        if (this.inProxyMode) {
            super.updateFloat(columnIndex, x);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        if (this.inProxyMode) {
            super.updateDouble(columnIndex, x);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        if (this.inProxyMode) {
            super.updateBigDecimal(columnIndex, x);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        if (this.inProxyMode) {
            super.updateString(columnIndex, x);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        if (this.inProxyMode) {
            super.updateBytes(columnIndex, x);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        if (this.inProxyMode) {
            super.updateDate(columnIndex, x);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        if (this.inProxyMode) {
            super.updateTime(columnIndex, x);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        if (this.inProxyMode) {
            super.updateTimestamp(columnIndex, x);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        if (this.inProxyMode) {
            super.updateAsciiStream(columnIndex, x, length);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        if (this.inProxyMode) {
            super.updateBinaryStream(columnIndex, x, length);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        if (this.inProxyMode) {
            super.updateCharacterStream(columnIndex, x, length);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        if (this.inProxyMode) {
            super.updateObject(columnIndex, x, scaleOrLength);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        if (this.inProxyMode) {
            super.updateObject(columnIndex, x);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        if (this.inProxyMode) {
            super.updateNull(columnLabel);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        if (this.inProxyMode) {
            super.updateBoolean(columnLabel, x);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        if (this.inProxyMode) {
            super.updateByte(columnLabel, x);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        if (this.inProxyMode) {
            super.updateShort(columnLabel, x);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        if (this.inProxyMode) {
            super.updateInt(columnLabel, x);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        if (this.inProxyMode) {
            super.updateLong(columnLabel, x);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        if (this.inProxyMode) {
            super.updateFloat(columnLabel, x);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        if (this.inProxyMode) {
            super.updateDouble(columnLabel, x);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        if (this.inProxyMode) {
            super.updateBigDecimal(columnLabel, x);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        if (this.inProxyMode) {
            super.updateString(columnLabel, x);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        if (this.inProxyMode) {
            super.updateBytes(columnLabel, x);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        if (this.inProxyMode) {
            super.updateDate(columnLabel, x);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        if (this.inProxyMode) {
            super.updateTime(columnLabel, x);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        if (this.inProxyMode) {
            super.updateTimestamp(columnLabel, x);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        if (this.inProxyMode) {
            super.updateAsciiStream(columnLabel, x, length);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        if (this.inProxyMode) {
            super.updateBinaryStream(columnLabel, x, length);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        if (this.inProxyMode) {
            super.updateCharacterStream(columnLabel, reader, length);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        if (this.inProxyMode) {
            super.updateObject(columnLabel, x, scaleOrLength);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        if (this.inProxyMode) {
            super.updateObject(columnLabel, x);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void insertRow() throws SQLException {
        if (this.inProxyMode) {
            super.insertRow();
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateRow() throws SQLException {
        if (this.inProxyMode) {
            super.updateRow();
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void deleteRow() throws SQLException {
        if (this.inProxyMode) {
            super.deleteRow();
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void refreshRow() throws SQLException {
        if (this.inProxyMode) {
            super.refreshRow();
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        if (this.inProxyMode) {
            super.cancelRowUpdates();
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        super.moveToInsertRow();
        this.inProxyMode = true;
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        super.moveToCurrentRow();
        this.inProxyMode = true;
    }

    @Override
    public java.sql.Statement getStatement() throws SQLException {
        if (this.inProxyMode) {
            return super.getStatement();
        }
        return this.statement;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        if (this.inProxyMode) {
            return super.getObject(columnIndex, map);
        }
        lastValueRead = null;
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        if (this.inProxyMode) {
            return super.getRef(columnIndex);
        }
        lastValueRead = null;
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        if (this.inProxyMode) {
            return super.getBlob(columnIndex);
        }
        lastValueRead = currentDataBlock.get(blockIdx.get())[columnIndex -1];
        Object objUUID = lastValueRead;
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
        if (this.inProxyMode) {
            return super.getClob(columnIndex);
        }
        lastValueRead = currentDataBlock.get(blockIdx.get())[columnIndex -1];
        String clobRefUUID = (String) lastValueRead;
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
        if (this.inProxyMode) {
            return super.getArray(columnIndex);
        }
        lastValueRead = null;
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        if (this.inProxyMode) {
            return super.getObject(columnLabel, map);
        }
        lastValueRead = null;
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        if (this.inProxyMode) {
            return super.getRef(columnLabel);
        }
        lastValueRead = null;
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        if (this.inProxyMode) {
            return super.getBlob(columnLabel);
        }
        lastValueRead = currentDataBlock.get(blockIdx.get())[this.labelsMap.get(columnLabel.toUpperCase())];
        String blobRefUUID = (String) lastValueRead;
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
        if (this.inProxyMode) {
            return super.getClob(columnLabel);
        }
        lastValueRead = null;
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        if (this.inProxyMode) {
            return super.getArray(columnLabel);
        }
        lastValueRead = null;
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        if (this.inProxyMode) {
            return super.getDate(columnIndex, cal);
        }
        lastValueRead = null;
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        if (this.inProxyMode) {
            return super.getDate(columnLabel, cal);
        }
        lastValueRead = null;
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        if (this.inProxyMode) {
            return super.getTime(columnIndex, cal);
        }
        lastValueRead = null;
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        if (this.inProxyMode) {
            return super.getTime(columnLabel, cal);
        }
        lastValueRead = null;
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        if (this.inProxyMode) {
            return super.getTimestamp(columnIndex, cal);
        }
        lastValueRead = null;
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        if (this.inProxyMode) {
            return super.getTimestamp(columnLabel, cal);
        }
        lastValueRead = null;
        throw new RuntimeException("Not implemented");
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        if (this.inProxyMode) {
            return super.getURL(columnIndex);
        }
        lastValueRead = currentDataBlock.get(blockIdx.get())[columnIndex -1];
        return (URL) lastValueRead;
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        if (this.inProxyMode) {
            return super.getURL(columnLabel);
        }
        lastValueRead = currentDataBlock.get(blockIdx.get())[this.labelsMap.get(columnLabel.toUpperCase())];
        return (URL) lastValueRead;
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        if (this.inProxyMode) {
            super.updateRef(columnIndex, x);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        if (this.inProxyMode) {
            super.updateRef(columnLabel, x);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        if (this.inProxyMode) {
            super.updateBlob(columnIndex, x);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        if (this.inProxyMode) {
            super.updateBlob(columnLabel, x);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        if (this.inProxyMode) {
            super.updateClob(columnIndex, x);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        if (this.inProxyMode) {
            super.updateClob(columnLabel, x);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        if (this.inProxyMode) {
            super.updateArray(columnIndex, x);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        if (this.inProxyMode) {
            super.updateArray(columnLabel, x);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        if (this.inProxyMode) {
            return super.getRowId(columnIndex);
        }
        lastValueRead = null;
        throw new RuntimeException("Not implemented");
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        if (this.inProxyMode) {
            return super.getRowId(columnLabel);
        }
        lastValueRead = null;
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        if (this.inProxyMode) {
            super.updateRowId(columnIndex, x);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        if (this.inProxyMode) {
            super.updateRowId(columnLabel, x);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public int getHoldability() throws SQLException {
        if (this.inProxyMode) {
            return super.getHoldability();
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public boolean isClosed() throws SQLException {
        if (this.inProxyMode) {
            return super.isClosed();
        }
        return this.closed;
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        if (this.inProxyMode) {
            super.updateNString(columnIndex, nString);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        if (this.inProxyMode) {
            super.updateNString(columnLabel, nString);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        if (this.inProxyMode) {
            super.updateNClob(columnIndex, nClob);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        if (this.inProxyMode) {
            super.updateNClob(columnLabel, nClob);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        if (this.inProxyMode) {
            return super.getNClob(columnIndex);
        }
        lastValueRead = null;
        throw new RuntimeException("Not implemented");
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        if (this.inProxyMode) {
            return super.getNClob(columnLabel);
        }
        lastValueRead = null;
        throw new RuntimeException("Not implemented");
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        if (this.inProxyMode) {
            return super.getSQLXML(columnIndex);
        }
        lastValueRead = null;
        throw new RuntimeException("Not implemented");
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        if (this.inProxyMode) {
            return super.getSQLXML(columnLabel);
        }
        lastValueRead = null;
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        if (this.inProxyMode) {
            super.updateSQLXML(columnIndex, xmlObject);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        if (this.inProxyMode) {
            super.updateSQLXML(columnLabel, xmlObject);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        if (this.inProxyMode) {
            return super.getNString(columnIndex);
        }
        lastValueRead = null;
        throw new RuntimeException("Not implemented");
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        if (this.inProxyMode) {
            return super.getNString(columnLabel);
        }
        lastValueRead = null;
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        if (this.inProxyMode) {
            return super.getNCharacterStream(columnIndex);
        }
        lastValueRead = null;
        throw new RuntimeException("Not implemented");
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        if (this.inProxyMode) {
            return super.getNCharacterStream(columnLabel);
        }
        lastValueRead = null;
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        if (this.inProxyMode) {
            super.updateNCharacterStream(columnIndex, x, length);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        if (this.inProxyMode) {
            super.updateNCharacterStream(columnLabel, reader, length);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        if (this.inProxyMode) {
            super.updateAsciiStream(columnIndex, x, length);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        if (this.inProxyMode) {
            super.updateBinaryStream(columnIndex, x, length);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        if (this.inProxyMode) {
            super.updateCharacterStream(columnIndex, x, length);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        if (this.inProxyMode) {
            super.updateAsciiStream(columnLabel, x, length);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        if (this.inProxyMode) {
            super.updateBinaryStream(columnLabel, x, length);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        if (this.inProxyMode) {
            super.updateCharacterStream(columnLabel, reader, length);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        if (this.inProxyMode) {
            super.updateBlob(columnIndex, inputStream, length);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        if (this.inProxyMode) {
            super.updateBlob(columnLabel, inputStream, length);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        if (this.inProxyMode) {
            super.updateClob(columnIndex, reader, length);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        if (this.inProxyMode) {
            super.updateClob(columnLabel, reader, length);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        if (this.inProxyMode) {
            super.updateNClob(columnIndex, reader, length);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        if (this.inProxyMode) {
            super.updateNClob(columnLabel, reader, length);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        if (this.inProxyMode) {
            super.updateNCharacterStream(columnIndex, x);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        if (this.inProxyMode) {
            super.updateNCharacterStream(columnLabel, reader);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        if (this.inProxyMode) {
            super.updateAsciiStream(columnIndex, x);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        if (this.inProxyMode) {
            super.updateBinaryStream(columnIndex, x);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        if (this.inProxyMode) {
            super.updateCharacterStream(columnIndex, x);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        if (this.inProxyMode) {
            super.updateAsciiStream(columnLabel, x);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        if (this.inProxyMode) {
            super.updateBinaryStream(columnLabel, x);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        if (this.inProxyMode) {
            super.updateCharacterStream(columnLabel, reader);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        if (this.inProxyMode) {
            super.updateBlob(columnIndex, inputStream);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        if (this.inProxyMode) {
            super.updateBlob(columnLabel, inputStream);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        //TODO review if we could fall back to proxy when these update methods are called, would have to reposition the curor to the current row being read before doing the update though
        if (this.inProxyMode) {
            super.updateClob(columnIndex, reader);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        if (this.inProxyMode) {
            super.updateClob(columnLabel, reader);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        if (this.inProxyMode) {
            super.updateNClob(columnIndex, reader);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        if (this.inProxyMode) {
            super.updateNClob(columnLabel, reader);
            return;
        }
        throw new RuntimeException("Not implemented");
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        if (this.inProxyMode) {
            return super.getObject(columnIndex, type);
        }
        lastValueRead = currentDataBlock.get(blockIdx.get())[columnIndex -1];
        return (T) lastValueRead;
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        if (this.inProxyMode) {
            return super.getObject(columnLabel, type);
        }
        lastValueRead = currentDataBlock.get(blockIdx.get())[this.labelsMap.get(columnLabel.toUpperCase())];
        return (T) lastValueRead;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        if (this.inProxyMode) {
            return super.isWrapperFor(iface);
        }
        throw new RuntimeException("Not implemented");
    }
}