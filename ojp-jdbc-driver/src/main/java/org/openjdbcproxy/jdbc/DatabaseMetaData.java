package org.openjdbcproxy.jdbc;

import com.google.protobuf.ByteString;
import com.openjdbcproxy.grpc.CallResourceRequest;
import com.openjdbcproxy.grpc.CallResourceResponse;
import com.openjdbcproxy.grpc.CallType;
import com.openjdbcproxy.grpc.ResourceType;
import com.openjdbcproxy.grpc.SessionInfo;
import com.openjdbcproxy.grpc.TargetCall;
import lombok.SneakyThrows;
import org.openjdbcproxy.grpc.client.StatementService;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.openjdbcproxy.grpc.SerializationHandler.deserialize;
import static org.openjdbcproxy.grpc.SerializationHandler.serialize;

public class DatabaseMetaData implements java.sql.DatabaseMetaData {
    private final StatementService statementService;
    private final org.openjdbcproxy.jdbc.Connection connection;
    private final Statement statement;

    public DatabaseMetaData(SessionInfo session, StatementService statementService,
                            org.openjdbcproxy.jdbc.Connection connection, Statement statement) {
        this.statementService = statementService;
        this.connection = connection;
        this.statement = statement;
    }


    private CallResourceRequest.Builder newCallBuilder() {
        return CallResourceRequest.newBuilder()
                .setSession(this.connection.getSession())
                .setResourceType(ResourceType.RES_CONNECTION)
                .setResourceUUID(this.connection.getSession().getSessionUUID());
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException("Unwrap not supported.");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLException("isWrappedFor not supported.");
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_ALL, "ProceduresAreCallable", Boolean.class);
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_ALL, "TablesAreSelectable", Boolean.class);
    }

    @Override
    public String getURL() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "URL", String.class);
    }

    @Override
    public String getUserName() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "UserName", String.class);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_IS, "ReadOnly", Boolean.class);
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_NULLS, "AreSortedHigh", Boolean.class);
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_NULLS, "AreSortedLow", Boolean.class);
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_NULLS, "AreSortedAtStart", Boolean.class);
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_NULLS, "AreSortedAtEnd", Boolean.class);
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "DatabaseProductName", String.class);
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "DatabaseProductVersion", String.class);
    }

    @Override
    public String getDriverName() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "DriverName", String.class);
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "DriverVersion", String.class);
    }

    @SneakyThrows
    @Override
    public int getDriverMajorVersion() {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "DriverMajorVersion", Integer.class);
    }

    @SneakyThrows
    @Override
    public int getDriverMinorVersion() {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "DriverMinorVersion", Integer.class);
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_USES, "LocalFiles", Boolean.class);
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_USES, "LocalFilePerTable", Boolean.class);
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "MixedCaseIdentifiers", Boolean.class);
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_STORES, "UpperCaseIdentifiers", Boolean.class);
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_STORES, "LowerCaseIdentifiers", Boolean.class);
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_STORES, "MixedCaseIdentifiers", Boolean.class);
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "MixedCaseQuotedIdentifiers", Boolean.class);
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_STORES, "UpperCaseQuotedIdentifiers", Boolean.class);
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_STORES, "LowerCaseQuotedIdentifiers", Boolean.class);
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_STORES, "MixedCaseQuotedIdentifiers", Boolean.class);
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "IdentifierQuoteString", String.class);
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "SQLKeywords", String.class);
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "NumericFunctions", String.class);
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "StringFunctions", String.class);
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "SystemFunctions", String.class);
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "TimeDateFunctions", String.class);
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "SearchStringEscape", String.class);
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "ExtraNameCharacters", String.class);
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "AlterTableWithAddColumn", Boolean.class);
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "AlterTableWithDropColumn", Boolean.class);
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "ColumnAliasing", Boolean.class);
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_NULL, "PlusNonNullIsNull", Boolean.class);
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "Convert", Boolean.class);
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "Convert", Boolean.class,
                Arrays.asList(fromType, toType));
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "TableCorrelationNames", Boolean.class);
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "DifferentTableCorrelationNames", Boolean.class);
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "ExpressionsInOrderBy", Boolean.class);
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "OrderByUnrelated", Boolean.class);
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "GroupBy", Boolean.class);
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "GroupByUnrelated", Boolean.class);
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "GroupByBeyondSelect", Boolean.class);
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "LikeEscapeClause", Boolean.class);
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "MultipleResultSets", Boolean.class);
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "MultipleTransactions", Boolean.class);
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "NonNullableColumns", Boolean.class);
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "MinimumSQLGrammar", Boolean.class);
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "CoreSQLGrammar", Boolean.class);
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "ExtendedSQLGrammar", Boolean.class);
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "ANSI92EntryLevelSQL", Boolean.class);
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "ANSI92IntermediateSQL", Boolean.class);
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "ANSI92FullSQL", Boolean.class);
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "IntegrityEnhancementFacility", Boolean.class);
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "OuterJoins", Boolean.class);
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "FullOuterJoins", Boolean.class);
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "LimitedOuterJoins", Boolean.class);
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "SchemaTerm", String.class);
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "ProcedureTerm", String.class);
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "CatalogTerm", String.class);
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_IS, "CatalogAtStart", Boolean.class);
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "CatalogSeparator", String.class);
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "SchemasInDataManipulation", Boolean.class);
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "SchemasInProcedureCalls", Boolean.class);
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "SchemasInTableDefinitions", Boolean.class);
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "SchemasInIndexDefinitions", Boolean.class);
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "SchemasInIndexDefinitions", Boolean.class);
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "CatalogsInDataManipulation", Boolean.class);
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "CatalogsInProcedureCalls", Boolean.class);
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "CatalogsInTableDefinitions", Boolean.class);
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "CatalogsInIndexDefinitions", Boolean.class);
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "CatalogsInPrivilegeDefinitions", Boolean.class);
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "PositionedDelete", Boolean.class);
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "PositionedUpdate", Boolean.class);
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "SelectForUpdate", Boolean.class);
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "StoredProcedures", Boolean.class);
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "SubqueriesInComparisons", Boolean.class);
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "SubqueriesInExists", Boolean.class);
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "SubqueriesInIns", Boolean.class);
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "SubqueriesInQuantifieds", Boolean.class);
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "CorrelatedSubqueries", Boolean.class);
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "Union", Boolean.class);
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "UnionAll", Boolean.class);
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "OpenCursorsAcrossCommit", Boolean.class);
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "OpenCursorsAcrossRollback", Boolean.class);
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "OpenStatementsAcrossCommit", Boolean.class);
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "OpenStatementsAcrossRollback", Boolean.class);
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "MaxBinaryLiteralLength", Integer.class);
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "MaxCharLiteralLength", Integer.class);
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "MaxColumnNameLength", Integer.class);
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "MaxColumnsInGroupBy", Integer.class);
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "MaxColumnsInIndex", Integer.class);
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "MaxColumnsInOrderBy", Integer.class);
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "MaxColumnsInSelect", Integer.class);
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "MaxColumnsInTable", Integer.class);
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "MaxConnections", Integer.class);
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "MaxCursorNameLength", Integer.class);
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "MaxIndexLength", Integer.class);
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "MaxSchemaNameLength", Integer.class);
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "MaxProcedureNameLength", Integer.class);
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "MaxCatalogNameLength", Integer.class);
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "MaxRowSize", Integer.class);
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_DOES, "MaxRowSizeIncludeBlobs", Boolean.class);
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "MaxStatementLength", Integer.class);
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "MaxStatements", Integer.class);
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "MaxTableNameLength", Integer.class);
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "MaxTablesInSelect", Integer.class);
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "MaxUserNameLength", Integer.class);
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "DefaultTransactionIsolation", Integer.class);
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "Transactions", Boolean.class);
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "TransactionIsolationLevel", Boolean.class,
            Arrays.asList(level));
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "DataDefinitionAndDataManipulationTransactions",
                Boolean.class);
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "DataManipulationTransactionsOnly", Boolean.class);
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_DATA, "DefinitionCausesTransactionCommit", Boolean.class);
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_DATA, "DefinitionIgnoredInTransactions", Boolean.class);
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        String resultSetUUID = this.retrieveMetadataAttribute(CallType.CALL_GET, "Procedures", String.class,
                Arrays.asList(catalog, schemaPattern, procedureNamePattern));
        return new RemoteProxyResultSet(resultSetUUID, this.statementService, this.connection, this.statement);
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        String resultSetUUID = this.retrieveMetadataAttribute(CallType.CALL_GET, "ProcedureColumns", String.class,
                Arrays.asList(catalog, schemaPattern, procedureNamePattern, columnNamePattern));
        return new RemoteProxyResultSet(resultSetUUID, this.statementService, this.connection, this.statement);
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        String resultSetUUID = this.retrieveMetadataAttribute(CallType.CALL_GET, "Tables", String.class,
                Arrays.asList(catalog, schemaPattern, tableNamePattern, types));
        return new RemoteProxyResultSet(resultSetUUID, this.statementService, this.connection, this.statement);
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        String resultSetUUID = this.retrieveMetadataAttribute(CallType.CALL_GET, "Schemas", String.class);
        return new RemoteProxyResultSet(resultSetUUID, this.statementService, this.connection, this.statement);
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        String resultSetUUID = this.retrieveMetadataAttribute(CallType.CALL_GET, "Catalogs", String.class);
        return new RemoteProxyResultSet(resultSetUUID, this.statementService, this.connection, this.statement);
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        String resultSetUUID = this.retrieveMetadataAttribute(CallType.CALL_GET, "TableTypes", String.class);
        return new RemoteProxyResultSet(resultSetUUID, this.statementService, this.connection, this.statement);
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        String resultSetUUID = this.retrieveMetadataAttribute(CallType.CALL_GET, "Columns", String.class,
                Arrays.asList(catalog, schemaPattern, tableNamePattern, columnNamePattern));
        return new RemoteProxyResultSet(resultSetUUID, this.statementService, this.connection, this.statement);
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        String resultSetUUID = this.retrieveMetadataAttribute(CallType.CALL_GET, "ColumnPrivileges", String.class,
                Arrays.asList(catalog, schema, table, columnNamePattern));
        return new RemoteProxyResultSet(resultSetUUID, this.statementService, this.connection, this.statement);
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        String resultSetUUID = this.retrieveMetadataAttribute(CallType.CALL_GET, "TablePrivileges", String.class,
                Arrays.asList(catalog, schemaPattern, tableNamePattern));
        return new RemoteProxyResultSet(resultSetUUID, this.statementService, this.connection, this.statement);
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        String resultSetUUID = this.retrieveMetadataAttribute(CallType.CALL_GET, "BestRowIdentifier", String.class,
                Arrays.asList(catalog, schema, table, scope, nullable));
        return new RemoteProxyResultSet(resultSetUUID, this.statementService, this.connection, this.statement);
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        String resultSetUUID = this.retrieveMetadataAttribute(CallType.CALL_GET, "VersionColumns", String.class,
                Arrays.asList(catalog, schema, table));
        return new RemoteProxyResultSet(resultSetUUID, this.statementService, this.connection, this.statement);
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        String resultSetUUID = this.retrieveMetadataAttribute(CallType.CALL_GET, "PrimaryKeys", String.class,
                Arrays.asList(catalog, schema, table));
        return new RemoteProxyResultSet(resultSetUUID, this.statementService, this.connection, this.statement);
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        String resultSetUUID = this.retrieveMetadataAttribute(CallType.CALL_GET, "ImportedKeys", String.class,
                Arrays.asList(catalog, schema, table));
        return new RemoteProxyResultSet(resultSetUUID, this.statementService, this.connection, this.statement);
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        String resultSetUUID = this.retrieveMetadataAttribute(CallType.CALL_GET, "ExportedKeys", String.class,
                Arrays.asList(catalog, schema, table));
        return new RemoteProxyResultSet(resultSetUUID, this.statementService, this.connection, this.statement);
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog,
                                       String foreignSchema, String foreignTable) throws SQLException {
        String resultSetUUID = this.retrieveMetadataAttribute(CallType.CALL_GET, "CrossReference", String.class,
                Arrays.asList(parentCatalog, parentSchema, parentTable, foreignCatalog, foreignSchema, foreignTable));
        return new RemoteProxyResultSet(resultSetUUID, this.statementService, this.connection, this.statement);
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        String resultSetUUID = this.retrieveMetadataAttribute(CallType.CALL_GET, "TypeInfo", String.class);
        return new RemoteProxyResultSet(resultSetUUID, this.statementService, this.connection, this.statement);
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        String resultSetUUID = this.retrieveMetadataAttribute(CallType.CALL_GET, "IndexInfo", String.class,
                Arrays.asList(catalog, schema, table, unique, approximate));
        return new RemoteProxyResultSet(resultSetUUID, this.statementService, this.connection, this.statement);
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "ResultSetType", Boolean.class,
                Arrays.asList(type));
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "ResultSetConcurrency", Boolean.class,
                Arrays.asList(type, concurrency));
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_OWN, "UpdatesAreVisible", Boolean.class,
                Arrays.asList(type));
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_OWN, "DeletesAreVisible", Boolean.class,
                Arrays.asList(type));
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_OWN, "InsertsAreVisible", Boolean.class,
                Arrays.asList(type));
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_OTHERS, "UpdatesAreVisible", Boolean.class,
                Arrays.asList(type));
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_OTHERS, "DeletesAreVisible", Boolean.class,
                Arrays.asList(type));
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_OTHERS, "InsertsAreVisible", Boolean.class,
                Arrays.asList(type));
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_UPDATES, "AreDetected", Boolean.class,
                Arrays.asList(type));
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_DELETES, "AreDetected", Boolean.class,
                Arrays.asList(type));
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_INSERTS, "AreDetected", Boolean.class,
                Arrays.asList(type));
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "BatchUpdates", Boolean.class);
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        String resultSetUUID = this.retrieveMetadataAttribute(CallType.CALL_GET, "UDTs", String.class,
                Arrays.asList(catalog, schemaPattern, typeNamePattern, types));
        return new RemoteProxyResultSet(resultSetUUID, this.statementService, this.connection, this.statement);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return (this.statement != null)? statement.getConnection() : this.connection;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "Savepoints", Boolean.class);
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "NamedParameters", Boolean.class);
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "MultipleOpenResults", Boolean.class);
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "GetGeneratedKeys", Boolean.class);
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        String resultSetUUID = this.retrieveMetadataAttribute(CallType.CALL_GET, "SuperTypes", String.class,
                Arrays.asList(catalog, schemaPattern, typeNamePattern));
        return new RemoteProxyResultSet(resultSetUUID, this.statementService, this.connection, this.statement);
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        String resultSetUUID = this.retrieveMetadataAttribute(CallType.CALL_GET, "SuperTables", String.class,
                Arrays.asList(catalog, schemaPattern, tableNamePattern));
        return new RemoteProxyResultSet(resultSetUUID, this.statementService, this.connection, this.statement);
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern)
            throws SQLException {
        String resultSetUUID = this.retrieveMetadataAttribute(CallType.CALL_GET, "Attributes", String.class,
                Arrays.asList(catalog, schemaPattern, typeNamePattern, attributeNamePattern));
        return new RemoteProxyResultSet(resultSetUUID, this.statementService, this.connection, this.statement);
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "ResultSetHoldability", Boolean.class,
                Arrays.asList(holdability));
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "ResultSetHoldability", Integer.class);
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "DatabaseMajorVersion", Integer.class);
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "DatabaseMinorVersion", Integer.class);
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "JDBCMajorVersion", Integer.class);
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "JDBCMinorVersion", Integer.class);
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "SQLStateType", Integer.class);
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_LOCATORS, "UpdateCopy", Boolean.class);
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "StatementPooling", Boolean.class);
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "RowIdLifetime", RowIdLifetime.class);
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        String resultSetUUID = this.retrieveMetadataAttribute(CallType.CALL_GET, "Schemas", String.class,
                Arrays.asList(catalog, schemaPattern));
        return new RemoteProxyResultSet(resultSetUUID, this.statementService, this.connection, this.statement);
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "StoredFunctionsUsingCallSyntax", Boolean.class);
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_AUTO, "CommitFailureClosesAllResultSets", Boolean.class);
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        String resultSetUUID = this.retrieveMetadataAttribute(CallType.CALL_GET, "ClientInfoProperties", String.class);
        return new RemoteProxyResultSet(resultSetUUID, this.statementService, this.connection, this.statement);
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        String resultSetUUID = this.retrieveMetadataAttribute(CallType.CALL_GET, "Functions", String.class,
                Arrays.asList(catalog, schemaPattern, functionNamePattern));
        return new RemoteProxyResultSet(resultSetUUID, this.statementService, this.connection, this.statement);
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        String resultSetUUID = this.retrieveMetadataAttribute(CallType.CALL_GET, "FunctionColumns", String.class,
                Arrays.asList(catalog, schemaPattern, functionNamePattern, columnNamePattern));
        return new RemoteProxyResultSet(resultSetUUID, this.statementService, this.connection, this.statement);
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        String resultSetUUID = this.retrieveMetadataAttribute(CallType.CALL_GET, "PseudoColumns", String.class,
                Arrays.asList(catalog, schemaPattern, tableNamePattern, columnNamePattern));
        return new RemoteProxyResultSet(resultSetUUID, this.statementService, this.connection, this.statement);
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GENERATED, "KeyAlwaysReturned", Boolean.class);
    }

    @Override
    public long getMaxLogicalLobSize() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_GET, "MaxLogicalLobSize", Long.class);
    }

    @Override
    public boolean supportsRefCursors() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "RefCursors", Boolean.class);
    }

    @Override
    public boolean supportsSharding() throws SQLException {
        return this.retrieveMetadataAttribute(CallType.CALL_SUPPORTS, "Sharding", Boolean.class);
    }

    private <T> T retrieveMetadataAttribute(CallType callType, String attrName, Class returnType) throws SQLException {
        return this.retrieveMetadataAttribute(callType, attrName, returnType, Constants.EMPTY_OBJECT_LIST);
    }

    private <T> T retrieveMetadataAttribute(CallType callType, String attrName, Class returnType, List<Object> params) throws SQLException {
        CallResourceRequest.Builder reqBuilder = this.newCallBuilder();
        reqBuilder
            .setTarget(
                TargetCall.newBuilder()
                        .setCallType(CallType.CALL_GET)
                        .setResourceName("MetaData")
                        .setNextCall(TargetCall.newBuilder()
                                .setCallType(callType)
                                .setResourceName(attrName)
                                .setParams(ByteString.copyFrom(serialize(params)))
                                .build())
                        .build()
        );
        CallResourceResponse response = this.statementService.callResource(reqBuilder.build());
        this.connection.setSession(response.getSession());

        return (T) deserialize(response.getValues().toByteArray(), returnType);
    }
}
