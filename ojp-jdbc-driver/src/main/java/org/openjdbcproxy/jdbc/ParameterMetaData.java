package org.openjdbcproxy.jdbc;

import java.sql.SQLException;

//TODO implement methods
public class ParameterMetaData implements java.sql.ParameterMetaData {
    @Override
    public int getParameterCount() throws SQLException {
        return 0;
    }

    @Override
    public int isNullable(int param) throws SQLException {
        return 0;
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        return false;
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        return 0;
    }

    @Override
    public int getScale(int param) throws SQLException {
        return 0;
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        return 0;
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        return "";
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        return "";
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        return 0;
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
