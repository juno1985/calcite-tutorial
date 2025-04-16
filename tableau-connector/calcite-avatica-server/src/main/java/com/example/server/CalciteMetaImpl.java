package com.example.server;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.avatica.ColumnMetaData;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class CalciteMetaImpl extends MetaImpl {
    private final Connection connection;
    private final Map<String, Statement> statements = new HashMap<>();
    private final Map<String, String> statementSql = new HashMap<>();

    public CalciteMetaImpl(Connection connection) {
        super((AvaticaConnection) connection);
        this.connection = connection;
    }

    @Override
    public Meta.StatementHandle prepare(Meta.ConnectionHandle ch, String sql, long maxRowCount) {
        Meta.StatementHandle handle = super.createStatement(ch);
        handle.signature = createSignature(sql);
        statementSql.put(String.valueOf(handle.id), sql);
        return handle;
    }

    @Override
    public Meta.ExecuteResult prepareAndExecute(Meta.StatementHandle h, String sql, long maxRowCount,
            PrepareCallback callback) throws NoSuchStatementException {
        return prepareAndExecute(h, sql, maxRowCount, -1, callback);
    }

    @Override
    public Meta.ExecuteResult prepareAndExecute(Meta.StatementHandle h, String sql, long maxRowCount,
            int maxRowsInFirstFrame, PrepareCallback callback) throws NoSuchStatementException {
        try {
            Statement statement = connection.createStatement();
            statements.put(String.valueOf(h.id), statement);
            statementSql.put(String.valueOf(h.id), sql);
            
            boolean isQuery = statement.execute(sql);
            if (isQuery) {
                ResultSet resultSet = statement.getResultSet();
                // Create signature with metadata BEFORE processing rows
                h.signature = createSignature(sql, resultSet);
                
                List<Object> rows = new ArrayList<>();
                int rowCount = 0;
                while (resultSet.next() && (maxRowCount < 0 || rowCount < maxRowCount)) {
                    List<Object> row = new ArrayList<>();
                    for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
                        Object value = resultSet.getObject(i);
                        // Handle null values appropriately
                        row.add(value != null ? value : "");
                    }
                    rows.add(row);
                    rowCount++;
                }
                
                Meta.Frame frame = Meta.Frame.create(0, true, rows);
                return new Meta.ExecuteResult(Collections.singletonList(
                    Meta.MetaResultSet.create(String.valueOf(h.id), 0, false, h.signature, frame)));
            } else {
                int updateCount = statement.getUpdateCount();
                return new Meta.ExecuteResult(Collections.singletonList(
                    Meta.MetaResultSet.count(String.valueOf(h.id), updateCount, 0L)));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Error executing query: " + e.getMessage(), e);
        }
    }

    @Override
    public Meta.ExecuteResult execute(Meta.StatementHandle h, List<TypedValue> parameterValues, int maxRowCount)
            throws NoSuchStatementException {
        return execute(h, parameterValues, (long) maxRowCount);
    }

    @Override
    public Meta.ExecuteResult execute(Meta.StatementHandle h, List<TypedValue> parameterValues, long maxRowCount)
            throws NoSuchStatementException {
        Statement statement = statements.get(String.valueOf(h.id));
        if (statement == null) {
            throw new NoSuchStatementException(h);
        }
        return prepareAndExecute(h, statementSql.get(String.valueOf(h.id)), maxRowCount, -1, null);
    }

    @Override
    public Meta.Frame fetch(Meta.StatementHandle h, long offset, int fetchMaxRowCount) throws NoSuchStatementException {
        try {
            Statement statement = statements.get(String.valueOf(h.id));
            if (statement == null) {
                throw new NoSuchStatementException(h);
            }

            ResultSet resultSet = statement.getResultSet();
            if (resultSet == null) {
                return Meta.Frame.EMPTY;
            }

            // Skip to the requested offset
            for (long i = 0; i < offset; i++) {
                if (!resultSet.next()) {
                    return Meta.Frame.EMPTY;
                }
            }

            List<Object> rows = new ArrayList<>();
            int rowCount = 0;
            while (resultSet.next() && rowCount < fetchMaxRowCount) {
                List<Object> row = new ArrayList<>();
                ResultSetMetaData metaData = resultSet.getMetaData();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    row.add(resultSet.getObject(i));
                }
                rows.add(row);
                rowCount++;
            }

            return Meta.Frame.create(offset, rowCount < fetchMaxRowCount, rows);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void closeStatement(Meta.StatementHandle h) {
        try {
            Statement statement = statements.remove(String.valueOf(h.id));
            statementSql.remove(String.valueOf(h.id));
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean syncResults(Meta.StatementHandle sh, QueryState state, long offset) {
        return false;
    }

    @Override
    public void commit(Meta.ConnectionHandle ch) {
        try {
            connection.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void rollback(Meta.ConnectionHandle ch) {
        try {
            connection.rollback();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Meta.ExecuteBatchResult executeBatch(Meta.StatementHandle h, List<List<TypedValue>> parameterValueLists)
            throws NoSuchStatementException {
        return new Meta.ExecuteBatchResult(new long[0]); // Minimal implementation
    }

    @Override
    public Meta.ExecuteBatchResult prepareAndExecuteBatch(Meta.StatementHandle h, List<String> sqlCommands)
            throws NoSuchStatementException {
        return new Meta.ExecuteBatchResult(new long[0]); // Minimal implementation
    }

    @Override
    public MetaResultSet getTables(ConnectionHandle ch, String catalog, Pat schemaPattern, Pat tableNamePattern, List<String> typeList) {
        try {
            System.out.println("\ngetTables called with:");
            System.out.println("  catalog: " + catalog);
            System.out.println("  schemaPattern: " + (schemaPattern == null ? "null" : schemaPattern.s));
            System.out.println("  tableNamePattern: " + (tableNamePattern == null ? "null" : tableNamePattern.s));
            System.out.println("  typeList: " + typeList);

            // Always include TABLE and VIEW types for Tableau compatibility
            if (typeList == null || typeList.isEmpty()) {
                typeList = Arrays.asList("TABLE", "VIEW");
            } else if (!typeList.contains("VIEW")) {
                List<String> newTypeList = new ArrayList<>(typeList);
                newTypeList.add("VIEW");
                typeList = newTypeList;
            }

            String schemaName = schemaPattern == null ? "%" : schemaPattern.s;
            String tableName = tableNamePattern == null ? "%" : tableNamePattern.s;

            System.out.println("\nQuerying metadata for schema: " + schemaName);
            ResultSet rs = connection.getMetaData().getTables(
                catalog,
                schemaName,
                tableName,
                typeList.toArray(new String[0]));

            System.out.println("\nMetadata query parameters:");
            System.out.println("  catalog=" + catalog);
            System.out.println("  schema=" + schemaName);
            System.out.println("  table=" + tableName);
            System.out.println("  types=" + typeList);

            System.out.println("\nProcessing result set:");
            List<Object> rows = new ArrayList<>();
            while (rs.next()) {
                String foundCatalog = rs.getString(1);
                String foundSchema = rs.getString(2);
                String foundTable = rs.getString(3);
                String foundType = rs.getString(4);
                System.out.println("  Found: catalog=" + foundCatalog + 
                                 ", schema=" + foundSchema + 
                                 ", table=" + foundTable + 
                                 ", type=" + foundType);

                List<Object> row = Arrays.asList(
                    foundCatalog,
                    foundSchema,
                    foundTable,
                    foundType,
                    rs.getString(5),  // REMARKS
                    null,  // TYPE_CAT
                    null,  // TYPE_SCHEM
                    null,  // TYPE_NAME
                    null,  // SELF_REFERENCING_COL_NAME
                    null   // REF_GENERATION
                );
                rows.add(row);
            }

            System.out.println("Total tables found: " + rows.size());

            List<ColumnMetaData> columns = Arrays.asList(
                columnMetaData(0, "TABLE_CAT", Types.VARCHAR, "String"),
                columnMetaData(1, "TABLE_SCHEM", Types.VARCHAR, "String"),
                columnMetaData(2, "TABLE_NAME", Types.VARCHAR, "String"),
                columnMetaData(3, "TABLE_TYPE", Types.VARCHAR, "String"),
                columnMetaData(4, "REMARKS", Types.VARCHAR, "String"),
                columnMetaData(5, "TYPE_CAT", Types.VARCHAR, "String"),
                columnMetaData(6, "TYPE_SCHEM", Types.VARCHAR, "String"),
                columnMetaData(7, "TYPE_NAME", Types.VARCHAR, "String"),
                columnMetaData(8, "SELF_REFERENCING_COL_NAME", Types.VARCHAR, "String"),
                columnMetaData(9, "REF_GENERATION", Types.VARCHAR, "String")
            );

            return createMetaResultSet(columns, rows);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MetaResultSet getSchemas(ConnectionHandle ch, String catalog, Pat schemaPattern) {
        try {
            System.out.println("\ngetSchemas called with:");
            System.out.println("  catalog: " + catalog);
            System.out.println("  schemaPattern: " + (schemaPattern == null ? "null" : schemaPattern.s));

            ResultSet rs = connection.getMetaData().getSchemas(
                catalog,
                schemaPattern == null ? "%" : schemaPattern.s);

            List<Object> rows = new ArrayList<>();
            while (rs.next()) {
                String foundSchema = rs.getString(1);
                String foundCatalog = rs.getString(2);
                System.out.println("  Found schema: " + foundSchema + " in catalog: " + foundCatalog);
                
                List<Object> row = Arrays.asList(
                    foundSchema,
                    foundCatalog
                );
                rows.add(row);
            }

            List<ColumnMetaData> columns = Arrays.asList(
                columnMetaData(0, "TABLE_SCHEM", Types.VARCHAR, "String"),
                columnMetaData(1, "TABLE_CATALOG", Types.VARCHAR, "String")
            );

            return createMetaResultSet(columns, rows);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MetaResultSet getColumns(ConnectionHandle ch, String catalog, Pat schemaPattern,
            Pat tableNamePattern, Pat columnNamePattern) {
        try {
            System.out.println("\ngetColumns called with:");
            System.out.println("  catalog: " + catalog);
            System.out.println("  schemaPattern: " + (schemaPattern == null ? "null" : schemaPattern.s));
            System.out.println("  tableNamePattern: " + (tableNamePattern == null ? "null" : tableNamePattern.s));
            System.out.println("  columnNamePattern: " + (columnNamePattern == null ? "null" : columnNamePattern.s));

            ResultSet rs = connection.getMetaData().getColumns(
                catalog,
                schemaPattern == null ? "%" : schemaPattern.s,
                tableNamePattern == null ? "%" : tableNamePattern.s,
                columnNamePattern == null ? "%" : columnNamePattern.s);

            List<Object> rows = new ArrayList<>();
            while (rs.next()) {
                String foundCatalog = rs.getString(1);
                String foundSchema = rs.getString(2);
                String foundTable = rs.getString(3);
                String foundColumn = rs.getString(4);
                System.out.println("  Found: column=" + foundColumn + 
                                 " in table=" + foundTable + 
                                 ", schema=" + foundSchema + 
                                 ", catalog=" + foundCatalog);

                List<Object> row = Arrays.asList(
                    foundCatalog,
                    foundSchema,
                    foundTable,
                    foundColumn,
                    rs.getInt(5),      // DATA_TYPE
                    rs.getString(6),   // TYPE_NAME
                    rs.getInt(7),      // COLUMN_SIZE
                    null,              // BUFFER_LENGTH
                    rs.getInt(9),      // DECIMAL_DIGITS
                    rs.getInt(10),     // NUM_PREC_RADIX
                    rs.getInt(11),     // NULLABLE
                    rs.getString(12),  // REMARKS
                    rs.getString(13),  // COLUMN_DEF
                    rs.getInt(14),     // SQL_DATA_TYPE
                    rs.getInt(15),     // SQL_DATETIME_SUB
                    rs.getInt(16),     // CHAR_OCTET_LENGTH
                    rs.getInt(17),     // ORDINAL_POSITION
                    rs.getString(18),  // IS_NULLABLE
                    null,              // SCOPE_CATALOG
                    null,              // SCOPE_SCHEMA
                    null,              // SCOPE_TABLE
                    null,              // SOURCE_DATA_TYPE
                    "NO"              // IS_AUTOINCREMENT
                );
                rows.add(row);
            }

            List<ColumnMetaData> columns = Arrays.asList(
                columnMetaData(0, "TABLE_CAT", Types.VARCHAR, "String"),
                columnMetaData(1, "TABLE_SCHEM", Types.VARCHAR, "String"),
                columnMetaData(2, "TABLE_NAME", Types.VARCHAR, "String"),
                columnMetaData(3, "COLUMN_NAME", Types.VARCHAR, "String"),
                columnMetaData(4, "DATA_TYPE", Types.INTEGER, "Integer"),
                columnMetaData(5, "TYPE_NAME", Types.VARCHAR, "String"),
                columnMetaData(6, "COLUMN_SIZE", Types.INTEGER, "Integer"),
                columnMetaData(7, "BUFFER_LENGTH", Types.INTEGER, "Integer"),
                columnMetaData(8, "DECIMAL_DIGITS", Types.INTEGER, "Integer"),
                columnMetaData(9, "NUM_PREC_RADIX", Types.INTEGER, "Integer"),
                columnMetaData(10, "NULLABLE", Types.INTEGER, "Integer"),
                columnMetaData(11, "REMARKS", Types.VARCHAR, "String"),
                columnMetaData(12, "COLUMN_DEF", Types.VARCHAR, "String"),
                columnMetaData(13, "SQL_DATA_TYPE", Types.INTEGER, "Integer"),
                columnMetaData(14, "SQL_DATETIME_SUB", Types.INTEGER, "Integer"),
                columnMetaData(15, "CHAR_OCTET_LENGTH", Types.INTEGER, "Integer"),
                columnMetaData(16, "ORDINAL_POSITION", Types.INTEGER, "Integer"),
                columnMetaData(17, "IS_NULLABLE", Types.VARCHAR, "String"),
                columnMetaData(18, "SCOPE_CATALOG", Types.VARCHAR, "String"),
                columnMetaData(19, "SCOPE_SCHEMA", Types.VARCHAR, "String"),
                columnMetaData(20, "SCOPE_TABLE", Types.VARCHAR, "String"),
                columnMetaData(21, "SOURCE_DATA_TYPE", Types.SMALLINT, "Short"),
                columnMetaData(22, "IS_AUTOINCREMENT", Types.VARCHAR, "String")
            );

            return createMetaResultSet(columns, rows);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private ColumnMetaData columnMetaData(int ordinal, String columnName, int type, String className) {
        return new ColumnMetaData(
            ordinal,
            false,
            true,
            false,
            false,
            DatabaseMetaData.columnNullable,
            true,
            type,
            columnName,
            columnName,
            null,
            0,
            0,
            null,
            null,
            ColumnMetaData.scalar(type, columnName, getRepFromSqlType(type)),
            true,
            false,
            false,
            className
        );
    }

    private MetaResultSet createMetaResultSet(List<ColumnMetaData> columns, List<Object> rows) {
        Meta.Signature signature = Meta.Signature.create(
            columns,
            "",
            Collections.emptyList(),
            Meta.CursorFactory.ARRAY,
            Meta.StatementType.SELECT
        );
        Meta.Frame frame = Meta.Frame.create(0, true, rows);
        return MetaResultSet.create(UUID.randomUUID().toString(), 0, false, signature, frame);
    }

    private MetaResultSet createEmptyResultSet() {
        return createMetaResultSet(Collections.emptyList(), Collections.emptyList());
    }

    private Meta.Signature createSignature(String sql) {
        return Meta.Signature.create(
            Collections.emptyList(),  // columns
            sql,                      // sql
            Collections.emptyList(),  // parameters
            Meta.CursorFactory.ARRAY, // cursorFactory
            Meta.StatementType.SELECT // statementType
        );
    }

    private Meta.Signature createSignature(String sql, ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        List<ColumnMetaData> columns = new ArrayList<>();
        
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            int columnType = metaData.getColumnType(i);
            ColumnMetaData.Rep rep = getRepFromSqlType(columnType);
            
            // Create scalar type with proper type mapping
            ColumnMetaData.ScalarType scalarType = ColumnMetaData.scalar(
                columnType,
                metaData.getColumnTypeName(i),
                rep);
            
            // Get precision and scale for numeric types
            int precision = 0;
            int scale = 0;
            if (columnType == Types.NUMERIC || columnType == Types.DECIMAL) {
                precision = metaData.getPrecision(i);
                scale = metaData.getScale(i);
            }
            
            String columnLabel = metaData.getColumnLabel(i);
            String columnName = metaData.getColumnName(i);
            String displayName = columnLabel != null && !columnLabel.isEmpty() ? columnLabel : columnName;
            
            columns.add(
                new ColumnMetaData(
                    i - 1,                           // ordinal
                    metaData.isAutoIncrement(i),    // autoIncrement
                    metaData.isCaseSensitive(i),    // caseSensitive
                    metaData.isSearchable(i),       // searchable
                    metaData.isCurrency(i),         // currency
                    metaData.isNullable(i),         // nullable
                    metaData.isSigned(i),           // signed
                    metaData.getColumnDisplaySize(i), // displaySize
                    displayName,                     // label
                    columnName,                      // columnName
                    metaData.getSchemaName(i),      // schemaName
                    precision,                       // precision
                    scale,                          // scale
                    metaData.getTableName(i),       // tableName
                    metaData.getCatalogName(i),     // catalogName
                    scalarType,                     // type
                    metaData.isReadOnly(i),         // readonly
                    metaData.isWritable(i),         // writable
                    metaData.isDefinitelyWritable(i), // definitelyWritable
                    metaData.getColumnClassName(i)   // columnClassName
                ));
        }
        
        return Meta.Signature.create(
            columns,
            sql,
            Collections.emptyList(),
            Meta.CursorFactory.ARRAY,
            Meta.StatementType.SELECT
        );
    }

    private ColumnMetaData.Rep getRepFromSqlType(int sqlType) {
        switch (sqlType) {
            case Types.BOOLEAN:
                return ColumnMetaData.Rep.BOOLEAN;
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
                return ColumnMetaData.Rep.INTEGER;
            case Types.BIGINT:
                return ColumnMetaData.Rep.LONG;
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
                return ColumnMetaData.Rep.DOUBLE;
            case Types.DECIMAL:
            case Types.NUMERIC:
                return ColumnMetaData.Rep.NUMBER;
            case Types.DATE:
                return ColumnMetaData.Rep.JAVA_SQL_DATE;
            case Types.TIME:
                return ColumnMetaData.Rep.JAVA_SQL_TIME;
            case Types.TIMESTAMP:
                return ColumnMetaData.Rep.JAVA_SQL_TIMESTAMP;
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return ColumnMetaData.Rep.STRING;
            default:
                return ColumnMetaData.Rep.OBJECT;
        }
    }
}
