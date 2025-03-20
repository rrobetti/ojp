package org.openjdbcproxy.grpc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Handles serialization of java objects to and from byte arrays.
 */
public class SerializationHandler {
    public static byte[] serialize(Object t) {
        try (ByteArrayOutputStream bo = new ByteArrayOutputStream()) {
            try (ObjectOutputStream so = new ObjectOutputStream(bo)) {
                so.writeObject(t);
                so.flush();
                return bo.toByteArray();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T deserialize(byte[] byteArray, Class<T> type) {
        try (ByteArrayInputStream bi = new ByteArrayInputStream(byteArray)) {
            try (ObjectInputStream si = new ObjectInputStream(bi)) {
                return type.cast(si.readObject());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
