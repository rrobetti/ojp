package org.openjdbcproxy.jdbc;

public class DbInfo {
    private static boolean H2DB;

    public synchronized static void setH2DB(boolean H2DB) {
        DbInfo.H2DB = H2DB;
    }

    public synchronized static boolean isH2DB() {
        return DbInfo.H2DB;
    }
}
