package org.openjdbcproxy.jdbc;

import javax.xml.transform.Result;
import javax.xml.transform.Source;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.SQLException;

//TODO to be implemented
public class SQLXML implements java.sql.SQLXML {
    @Override
    public void free() throws SQLException {

    }

    @Override
    public InputStream getBinaryStream() throws SQLException {
        return null;
    }

    @Override
    public OutputStream setBinaryStream() throws SQLException {
        return null;
    }

    @Override
    public Reader getCharacterStream() throws SQLException {
        return null;
    }

    @Override
    public Writer setCharacterStream() throws SQLException {
        return null;
    }

    @Override
    public String getString() throws SQLException {
        return "";
    }

    @Override
    public void setString(String value) throws SQLException {

    }

    @Override
    public <T extends Source> T getSource(Class<T> sourceClass) throws SQLException {
        return null;
    }

    @Override
    public <T extends Result> T setResult(Class<T> resultClass) throws SQLException {
        return null;
    }
}
