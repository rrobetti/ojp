package org.openjdbcproxy.constants;

public class CommonConstants {
    public static final int ROWS_PER_RESULT_SET_DATA_BLOCK = 100;
    public static final int MAX_LOB_DATA_BLOCK_SIZE = 999999999;//TODO temp for H2 db as per it does not support creating blobs in stages should be 1KB per block
}
