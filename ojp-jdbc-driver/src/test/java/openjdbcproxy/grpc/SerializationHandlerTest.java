package openjdbcproxy.grpc;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.openjdbcproxy.grpc.SerializationHandler.deserialize;
import static org.openjdbcproxy.grpc.SerializationHandler.serialize;

public class SerializationHandlerTest {
    @Test
    public void serializeDeserializeSuccessful() {
        //FOR
        Exception eOriginal = new RuntimeException("testing exception");

        //WHEN
        byte[] byteArray = serialize(eOriginal);
        Exception eDeserialized = deserialize(byteArray, Exception.class);

        //THEN
        assertEquals(eOriginal.getMessage(), eDeserialized.getMessage());
        assertEquals(eOriginal.getCause(), eDeserialized.getCause());
        StackTraceElement[] stackTraceOriginal = eOriginal.getStackTrace();
        StackTraceElement[] stackTraceDeserialized = eOriginal.getStackTrace();
        for (int i = 0; i < stackTraceOriginal.length; i++) {
            assertEquals(stackTraceOriginal[i], stackTraceDeserialized[i]);
        }
        assertNotEquals(eOriginal, eDeserialized);
    }
}
