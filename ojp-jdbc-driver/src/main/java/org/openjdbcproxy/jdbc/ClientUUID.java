package org.openjdbcproxy.jdbc;

import lombok.experimental.UtilityClass;

import java.util.UUID;

@UtilityClass
public class ClientUUID {
    private static final String _UUID = UUID.randomUUID().toString();

    /**
     * Return the current client UUID, every time the application restarts a new UUID is generated and lasts while the
     * application is not shutdown.
     * @return Client UUID
     */
    public String getUUID() {
        return _UUID;
    }
}
