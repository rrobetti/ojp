package org.openjdbcproxy.grpc.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Reusable constants. Mainly to save memory by creating less repeated objects.
 */
public class Constants {
    public static final String USER = "user";
    public static final String PASSWORD = "password";
    public static final String SHA_256 = "SHA-256";
    public static final String OJP_DRIVER_PREFIX = "ojp_";
    public static final String EMPTY_STRING = "";
    public static final List EMPTY_LIST = new ArrayList<>();
    public static final Map EMPTY_MAP = new HashMap<>();
    public static final String H2_DRIVER_CLASS = "org.h2.Driver";
    public static final String POSTGRES_DRIVER_CLASS = "org.postgresql.Driver";
}
