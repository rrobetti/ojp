package org.openjdbcproxy.jdbc;

/**
 * Identifies if a given session shall be terminated or not based on the known pattern of connection pools hygiene
 * calls before returning a connection to the pool, in other words connection pools don't close connections, they
 * close the attached resources like result sets and statements, rollback current transaction, set autocommit back to true,
 * set read only back to false and clear warnings. This class monitors which of these methods have been called and if it
 * is identified that the connection has been homogenized, a session termination trigger flag is set.
 * Note that rollback is not part of the trigger because it is a commonly called method and already managed specifically,
 * meaning that if rollback was called we already sent a message to the server and the transaction is rollback already.
 */
public class SessionTerminationTrigger {
    private boolean autocommitTrue = false;
    private boolean readOnlyFalse = false;
    private boolean clearWarningsCalled = false;

    /**
     * Only one flag is expected to be set as true in a given call, which in turn will update the internal flag
     * once all three flags are true this method will return true.
     * @param autocommitTrue
     * @param readOnlyFalse
     * @param clearWarningsCalled
     * @return boolean trigger has been issued.
     */
    public synchronized boolean triggerIssued(Boolean autocommitTrue, Boolean readOnlyFalse, Boolean clearWarningsCalled) {
        if (autocommitTrue != null) {
            this.autocommitTrue = autocommitTrue;
        }
        if (readOnlyFalse != null) {
            this.readOnlyFalse = readOnlyFalse;
        }
        if (clearWarningsCalled != null) {
            this.clearWarningsCalled = clearWarningsCalled;
        }

        return this.autocommitTrue && this.readOnlyFalse && this.clearWarningsCalled;
    }
}
