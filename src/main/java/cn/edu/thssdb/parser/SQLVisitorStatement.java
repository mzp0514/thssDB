package cn.edu.thssdb.parser;

import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.type.ComparisonType;
import cn.edu.thssdb.type.JoinType;
import cn.edu.thssdb.utils.Global;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;
import org.antlr.v4.runtime.tree.TerminalNode;

import javax.xml.crypto.Data;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.rmi.server.ExportException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;

public class SQLVisitorStatement extends SQLBaseVisitor<QueryResult> {

    private Database db;
    private long sessionID;

    public SQLVisitorStatement(Database db, long sessionID) {
        this.db = db;
        this.sessionID = sessionID;
    }


//    @Override
//    public QueryResult visitParse(SQLParser.ParseContext ctx) {
//        return null;
//    }
//
//    @Override
//    public QueryResult visitSql_stmt_list(SQLParser.Sql_stmt_listContext ctx) {
//        return null;
//    }
//
//    @Override
//    public QueryResult visitSql_stmt(SQLParser.Sql_stmtContext ctx) {
//        return null;
//    }

    @Override
    public QueryResult visitCreate_db_stmt(SQLParser.Create_db_stmtContext ctx) {
        if (this.db.txManager.getTransactionState(this.sessionID)) {
            return new QueryResult(String.format("Create failed, transaction mode does not support this statement"));
        }
        String dbName = ctx.database_name().getText().toLowerCase();
        if (Manager.getInstance().isDBExists(dbName))
            return new QueryResult(String.format("Create failed, database %s already exists", dbName));
        try {
            Manager.getInstance().createDatabaseIfNotExists(dbName);
        } catch (Exception e) {
            return new QueryResult("Create Database Failed: " + e.getMessage());
        }
        return new QueryResult(String.format("Create database %s successfully", dbName));
    }

    @Override
    public QueryResult visitDrop_db_stmt(SQLParser.Drop_db_stmtContext ctx) {
        if (this.db.txManager.getTransactionState(this.sessionID)) {
            return new QueryResult(String.format("Delete failed, transaction mode does not support this statement"));
        }
        String dbName = ctx.database_name().getText().toLowerCase();
        if (!Manager.getInstance().isDBExists(dbName))
            return new QueryResult(String.format("Delete failed, database %s not exists", dbName));
        try {
            Manager.getInstance().deleteDatabase(dbName);
        } catch (Exception e) {
            return new QueryResult("Delete Database Failed: " + e.getMessage());
        }
        return new QueryResult(String.format("Delete database %s successfully", dbName));
    }

    @Override
    public QueryResult visitCreate_user_stmt(SQLParser.Create_user_stmtContext ctx) {
        return null;
    }

    @Override
    public QueryResult visitDrop_user_stmt(SQLParser.Drop_user_stmtContext ctx) {
        return null;
    }

    @Override
    public QueryResult visitCreate_table_stmt(SQLParser.Create_table_stmtContext ctx) {
        if (this.db.txManager.getTransactionState(this.sessionID)) {
            return new QueryResult(String.format("Create failed, transaction mode does not support this statement"));
        }
        String tableName = ctx.table_name().getText().toLowerCase();
        if (this.db.tableInDB(tableName))
            return new QueryResult(String.format("Create failed, table %s already exists.", tableName));

        ArrayList<String> fieldNames = new ArrayList<>();
        ArrayList<ColumnType> fieldTypes = new ArrayList<>();
        ArrayList<Integer> maxLength = new ArrayList<>();
        ArrayList<Integer> primaryKey = new ArrayList<>();
        ArrayList<Boolean> notNull = new ArrayList<>();


        boolean isPrimayKeySet = false;

        List<SQLParser.Column_defContext> columnList = ctx.column_def();

        for (int i = 0; i < ctx.column_def().size(); i++)
        {
            SQLParser.Column_defContext column = columnList.get(i);

            // 索引名
            String fieldName = column.column_name().IDENTIFIER().getText().toLowerCase();
            if (fieldNames.contains(fieldName))
                return new QueryResult(String.format("Create Failed, duplicate field name %s", fieldName));
            fieldNames.add(fieldName);

            // 索引属性
            if (column.type_name().T_INT() != null)
            {
                fieldTypes.add(ColumnType.INT);
                maxLength.add(0);
            }
            else if (column.type_name().T_LONG() != null)
            {
                fieldTypes.add(ColumnType.LONG);
                maxLength.add(0);
            }
            else if (column.type_name().T_DOUBLE() != null)
            {
                fieldTypes.add(ColumnType.DOUBLE);
                maxLength.add(0);
            }
            else if (column.type_name().T_FLOAT() != null)
            {
                fieldTypes.add(ColumnType.FLOAT);
                maxLength.add(0);
            }
            else if (column.type_name().T_STRING() != null)
            {
                if (column.type_name().NUMERIC_LITERAL() != null)
                {
                    String strLen = column.type_name().NUMERIC_LITERAL().getText();
                    if (strLen.contains("."))
                        return new QueryResult("Create Failed, invalid length of the String Type");
                    fieldTypes.add(ColumnType.STRING);
                    maxLength.add(Integer.valueOf(strLen));
                }
                else
                {
                    return new QueryResult("Create Failed, You must assign the maxlength of String Type");
                }

            }

            boolean curColPrimary = false;
            boolean curColNotNull = false;
            List<SQLParser.Column_constraintContext> columnConstraintContextList = column.column_constraint();
            for (SQLParser.Column_constraintContext columnConstraint : columnConstraintContextList)
            {
                // 主键设置
                if (columnConstraint.K_PRIMARY() != null && columnConstraint.K_KEY() != null)
                {
                    if (isPrimayKeySet)
                        return new QueryResult("Create Failed, the definition have multiple primary keys");
                    isPrimayKeySet = true;
                    curColPrimary = true;
                    primaryKey.add(1);
                }


                // 非空设置
                if (columnConstraint.K_NOT() != null && columnConstraint.K_NULL() != null)
                {
                    curColNotNull = true;
                    notNull.add(true);
                }
            }
            if (!curColPrimary)
                primaryKey.add(0);
            if (!curColNotNull)
                notNull.add(false);

        }

        SQLParser.Table_constraintContext tableConstraint = ctx.table_constraint();
        List<SQLParser.Column_nameContext> primaryColumns = tableConstraint.column_name();
        if (primaryColumns.size() == 1)
        {
            if (isPrimayKeySet)
                return new QueryResult("Create Failed, the definition have multiple primary keys");
            String primaryColName = primaryColumns.get(0).getText().toLowerCase();
            int index = fieldNames.indexOf(primaryColName);
            if (index >= 0)
            {
                primaryKey.set(index, 1);
            }
            else
                return new QueryResult(String.format("Create Failed, can not find key %s", primaryColName));
        }
        else if (primaryColumns.size() > 1)
            return new QueryResult("Create Failed, the definition have multiple primary keys");

        Column[] columns = new Column[fieldNames.size()];
        for (int i = 0; i < columns.length; i++)
        {
            columns[i] = new Column(fieldNames.get(i), fieldTypes.get(i), primaryKey.get(i), notNull.get(i), maxLength.get(i));
        }

        try {
            this.db.create(tableName, columns);
        } catch (IOException e) {
            return new QueryResult("Create Failed, Internal IO Error");
        }


        return new QueryResult(String.format("Create table %s successfully", tableName));
    }

    @Override
    public QueryResult visitShow_meta_stmt(SQLParser.Show_meta_stmtContext ctx) {
        String tableName = ctx.table_name().getText().toLowerCase();
        if (this.db.tableInDB(tableName))
        {
            try {
                ArrayList<Column> cols = this.db.getTable(tableName).getColumns();
                StringBuilder res = new StringBuilder();
                for (Column col : cols)
                {
                    res.append("ColName: ").append(col.getName()).append(", ");
                    if (col.getType() != ColumnType.STRING)
                        res.append("Type: ").append(col.getType()).append('\n');
                    else
                        res.append("Type: ").append(col.getType()).append('(').append(col.getMaxLength()).append(')').append('\n');
                }
                return new QueryResult(res.toString());
            } catch (IOException | ClassNotFoundException e) {
                return new QueryResult("Shaw Table Failed: " + e.getMessage());
            }
        }
        else
            return new QueryResult(String.format("Table: %s not exists", tableName));
    }

    @Override
    public QueryResult visitGrant_stmt(SQLParser.Grant_stmtContext ctx) {
        return null;
    }

    @Override
    public QueryResult visitRevoke_stmt(SQLParser.Revoke_stmtContext ctx) {
        return null;
    }

    @Override
    public QueryResult visitUse_db_stmt(SQLParser.Use_db_stmtContext ctx) {
        if (this.db.txManager.getTransactionState(this.sessionID)) {
            return new QueryResult(String.format("Switch database failed, transaction mode does not support this statement"));
        }
        String dbName = ctx.database_name().getText().toLowerCase();
        if (!Manager.getInstance().isDBExists(dbName))
            return new QueryResult(String.format("Switch database failed, database %s not exists", dbName));
        try {
            //切换数据库时原数据库的数据表持久化处理
            this.db.txManager.persistTable(this.sessionID);

            this.db = Manager.getInstance().switchDatabase(dbName, sessionID);
            this.db.txManager.insertSession(sessionID);
        } catch (Exception e) {
            return new QueryResult("Switch Database Failed: " + e.getMessage());
        }
        return new QueryResult(String.format("Switch to database %s successfully"));

    }

    public ComparisonType getComparisonType(String op){
        switch (op){
            case "=":
                return ComparisonType.EQUAL;
            case "<":
                return ComparisonType.LESS;
            case ">":
                return ComparisonType.GREATER;
            case "<=":
                return ComparisonType.NGREATER;
            case ">=":
                return ComparisonType.NLESS;
            case "<>":
                return ComparisonType.NEQUAL;
            default:
                return ComparisonType.UNDECODE;

        }

    }

    @Override
    public QueryResult visitDelete_stmt(SQLParser.Delete_stmtContext ctx) {
        String tableName = ctx.table_name().getText().toLowerCase();
        if (this.db.tableInDB(tableName))
        {
            TableP table;
            try{
                table = this.db.getTable(tableName);
                if (table.currentSessionID != this.sessionID){
                    if (table.currentSessionID == -1){
                        table.currentSessionID = this.sessionID;
                    } else {
                        return new QueryResult("Delete Failed, current table has been locked");
                    }
                }
                ArrayList<Row> rowsToDelete;
                QueryResult mQuery = new QueryResult(table, null, true);
                if (ctx.K_WHERE()!=null){
                    String attrName = ctx.multiple_condition().condition().getChild(0).getText().toLowerCase();
                    Object attrValue;
                    String attrValueOrigin = ctx.multiple_condition().condition().getChild(2).getText();
                    ArrayList<Column> allCol = table.columns;
                    int loc = -1;
                    for (int i=0; i<allCol.size(); i++){
                        if (attrName.compareTo(allCol.get(i).getName())==0){
                            loc = i;
                            break;
                        }
                    }
                    if (loc == -1){
                        return new QueryResult("Delete Failed, Unknown Key");
                    } else {
                        switch (allCol.get(loc).getType()){
                            case INT:
                                attrValue = Integer.valueOf(attrValueOrigin);
                                break;
                            case DOUBLE:
                                attrValue = Double.valueOf(attrValueOrigin);
                                break;
                            case FLOAT:
                                attrValue = Float.valueOf(attrValueOrigin);
                                break;
                            case LONG:
                                attrValue = Long.valueOf(attrValueOrigin);
                                break;
                            case STRING:
                            default:
                                String text = attrValueOrigin.substring(1, attrValueOrigin.length() - 1);
                                int maxLength = allCol.get(loc).getMaxLength();
                                if (text.length() > maxLength)
                                    return new QueryResult(String.format("Insert Error: String Length Out of Bound for Column %s", allCol.get(loc).getName()));
                                attrValue = Global.resize(text, maxLength);
                                break;
                        }
                    }
                    ComparisonType op = getComparisonType(ctx.multiple_condition().condition().getChild(1).getText());
                    if (op == ComparisonType.UNDECODE){
                        return new QueryResult("Delete Failed, Unsupported comparison type");
                    }
                    rowsToDelete = mQuery.queryVal_s(attrName, op, attrValue);
                } else {
                    rowsToDelete = mQuery.queryAll_s();
                }

                if (!rowsToDelete.isEmpty()) {
                    table.delete(rowsToDelete);

                    if (this.db.txManager.getTransactionState(this.sessionID)) {
                        table.rowsForActions.push(rowsToDelete);
                        table.actionType.push(Global.STATE_TYPE.DELETE);
                        this.db.walManager.addStatement(this.db.getCurrentStatement());
                    } else {
                        //table.persist();
                        this.db.walManager.persist(this.db.getCurrentStatement());
                        table.currentSessionID = -1;
                    }
                    return new QueryResult(String.format("Delete %d row(s) successfully", rowsToDelete.size()));
                } else {
                    return new QueryResult("Delete Failed, No such conditions");
                }


            }
            catch (Exception e){
                return new QueryResult("Delete Failed: " + e.getMessage());
            }
        }
        else
        {
            return new QueryResult(String.format("Delete Failed, Unknown table name: %s", tableName));
        }

    }

    @Override
    public QueryResult visitDrop_table_stmt(SQLParser.Drop_table_stmtContext ctx) {
        if (this.db.txManager.getTransactionState(this.sessionID)) {
            return new QueryResult("Drop failed, transaction mode does not support this statement");
        }

        String tableName = ctx.table_name().getText().toLowerCase();
        if (this.db.tableInDB(tableName))
        {
            try{
                this.db.drop(tableName);
            } catch (IOException e) {
                return new QueryResult("Drop Failed, internal IO Error");
            }
            return new QueryResult(String.format("Drop table %s successfully", tableName));
        }
        else
        {
            return new QueryResult(String.format("Drop Error: Unknown table name %s", tableName));
        }
    }

    @Override
    public QueryResult visitShow_db_stmt(SQLParser.Show_db_stmtContext ctx) {
        return null;
    }

    @Override
    public QueryResult visitQuit_stmt(SQLParser.Quit_stmtContext ctx) {
        return null;
    }

    @Override
    public QueryResult visitShow_table_stmt(SQLParser.Show_table_stmtContext ctx) {
        return null;
    }

    @Override
    public QueryResult visitInsert_stmt(SQLParser.Insert_stmtContext ctx) {
        String tableName = ctx.table_name().getText().toLowerCase();
        if (this.db.tableInDB(tableName))
        {
            TableP table;
            try {
                table = this.db.getTable(tableName);
                if (table.currentSessionID != this.sessionID){
                    if (table.currentSessionID == -1){
                        table.currentSessionID = this.sessionID;
                    } else {
                        return new QueryResult("Insert Failed, current table has been locked");
                    }
                }

                List<SQLParser.Column_nameContext> colsToInsert = ctx.column_name();
                List<SQLParser.Value_entryContext> rowsToInsert = ctx.value_entry();
                HashSet<String> colNameSet = ctx.column_name().stream().map(x -> x.IDENTIFIER().getText().toLowerCase()).collect(Collectors.toCollection(HashSet::new));
                int[] insertOrder = new int[table.columns.size()];
                ArrayList<Column> allCol = table.columns;
                ArrayList<String> allColNames = table.columns.stream().map(Column::getName).collect(Collectors.toCollection(ArrayList::new));
                for (int i = 0; i < insertOrder.length; i++)
                    insertOrder[i] = i;

                if (rowsToInsert.size() == 0)
                    return new QueryResult("Insert Error: No Entrys!");

                // 自定义插入顺序
                if (colNameSet.size() != 0)
                {
                    // 检查重复
                    if (colNameSet.size() != ctx.column_name().size())
                        return new QueryResult("Insert Error: Duplicate Column Names!");

                    // 检查缺失的列，以及判断缺失的列是否为not null或主键
                    for (int i = 0; i < allCol.size(); i++)
                    {
                        Column col = allCol.get(i);
                        if (!colNameSet.contains(col.getName()))
                        {
                            if (allCol.get(i).isPrimary() || col.isNotNull())
                                return new QueryResult(String.format("Insert Error: Field %s can not be null", col.getName()));
                            insertOrder[i] = -1;
                        }
                    }

                    // 重组插入顺序
                    for (int i = 0; i < colsToInsert.size(); i++)
                    {
                        SQLParser.Column_nameContext col = colsToInsert.get(i);
                        int index = allColNames.indexOf(col.getText().toLowerCase());
                        if (index < 0)
                            return new QueryResult(String.format("Insert Error: Unknown Filed %s", col.getText()));
                        insertOrder[index] = i;
                    }

                }

                ArrayList<Row> rowsN = new ArrayList<>();
                for (SQLParser.Value_entryContext row : rowsToInsert)
                {
                    List<SQLParser.Literal_valueContext> entriesToInsert = row.literal_value();
                    if (colsToInsert.size() == 0 && entriesToInsert.size() != allCol.size())
                        return new QueryResult("Insert Error: Entries Size and Col Size don't match");
                    else if (colsToInsert.size() != 0 && entriesToInsert.size() != colsToInsert.size())
                        return new QueryResult("Insert Error: Entries Size and Col Size don't match");
                    Entry[] entries = new Entry[allCol.size()];
                    for (int i = 0; i < insertOrder.length; i++)
                    {
                        if (insertOrder[i] == -1)
                        {
                            entries[i] = new Entry(null);
                        }
                        else
                        {
                            SQLParser.Literal_valueContext entry = entriesToInsert.get(insertOrder[i]);
                            String text = entry.getText();
                            switch (allCol.get(i).getType()) {
                                case INT:
                                    entries[i] = new Entry(Integer.valueOf(text));
                                    break;
                                case FLOAT:
                                    entries[i] = new Entry(Float.valueOf(text));
                                    break;
                                case LONG:
                                    entries[i] = new Entry(Long.valueOf(text));
                                    break;
                                case DOUBLE:
                                    entries[i] = new Entry(Double.valueOf(text));
                                    break;
                                case STRING:
                                    text = text.substring(1, text.length() - 1);
                                    if (text.length() > allCol.get(i).getMaxLength())
                                        return new QueryResult(String.format("Insert Error: String Length Out of Bound for Column %s", allColNames.get(i)));
                                    if (text.length() == 0)
                                        return new QueryResult(String.format("Insert Error: Field %s can not be null", allColNames.get(i)));
                                    int maxLength = allCol.get(i).getMaxLength();
                                    entries[i] = new Entry(Global.resize(text, maxLength));
                                    break;
                                default:
                                    break;
                            }
                        }
                    }
                    Row r = new Row(entries);
                    if (this.db.txManager.getTransactionState(this.sessionID)) {
                        rowsN.add(r);
                    }
                    table.insert(r);
                }
                if (this.db.txManager.getTransactionState(this.sessionID)) {
                    table.rowsForActions.push(rowsN);
                    table.actionType.push(Global.STATE_TYPE.INSERT);
                    this.db.walManager.addStatement(this.db.getCurrentStatement());
                } else {
                    //table.persist();
                    this.db.walManager.persist(this.db.getCurrentStatement());
                    table.currentSessionID = -1;
                }
                return new QueryResult(String.format("Insert %d row(s) successfully", rowsToInsert.size()));

            } catch (Exception e) {
                return new QueryResult("Insert Failed: " + e.getMessage());
            }

        }
        else
            return new QueryResult(String.format("Insert Failed, Unknown table name: %s", tableName));
    }

    @Override
    public QueryResult visitValue_entry(SQLParser.Value_entryContext ctx) {
        return null;
    }

    @Override
    public QueryResult visitSelect_stmt(SQLParser.Select_stmtContext ctx) {
        if (ctx.table_query().get(0).K_JOIN().size()!=0) {
            //多表select语句处理
            ArrayList<TableP> queryTables = new ArrayList<TableP>();

            SQLParser.Table_queryContext joinStmt = ctx.table_query().get(0);
            List<SQLParser.Table_nameContext> tableName = joinStmt.table_name();
            for (int i=0; i<tableName.size(); i++){
                String name = tableName.get(i).getText().toLowerCase();
                if (this.db.tableInDB(name)){
                    try {
                        queryTables.add(this.db.getTable(name));
                    } catch (Exception e) {
                        return new QueryResult("Select Failed: " + e.getMessage());
                    }
                } else {
                    return new QueryResult(String.format("Select Failed, Unknown table name: %s", tableName));
                }
            }
            try {
                QueryResult mQuery;

                ArrayList<String> columnNames = new ArrayList<String>();
                ctx.result_column().forEach(it -> {
                    columnNames.add(it.getText().toLowerCase());
                });

                if (ctx.table_query().get(0).K_NATURAL()!=null) {
                    if (columnNames.get(0).compareTo("*") == 0) {
                        mQuery = new QueryResult(queryTables, null, null, null, JoinType.NATURAL_JOIN, true);
                    } else {
                        mQuery = new QueryResult(queryTables, columnNames, null, null, JoinType.NATURAL_JOIN, false);
                    }
                } else {
                    String attrName1 = joinStmt.multiple_condition().condition().getChild(0).getText().toLowerCase();
                    String attrName2 = joinStmt.multiple_condition().condition().getChild(2).getText().toLowerCase();
                    if (joinStmt.multiple_condition().condition().getChild(1).getText().compareTo("=") != 0) {
                        return new QueryResult("Select Failed, Unsupported On statement");
                    }
                    if (columnNames.get(0).compareTo("*") == 0) {
                        mQuery = new QueryResult(queryTables, null, attrName1, attrName2, JoinType.INNER_JOIN, true);
                    } else {
                        mQuery = new QueryResult(queryTables, columnNames, attrName1, attrName2, JoinType.INNER_JOIN, false);
                    }
                }
                ArrayList<Row> rowsToSelect;
                if (ctx.K_WHERE()!=null){
                    String attrName = ctx.multiple_condition().condition().getChild(0).getText().toLowerCase();
                    String attrValueOrigin = ctx.multiple_condition().condition().getChild(2).getText();
                    Object attrValue;
                    String[] s = attrName.split("\\.");
                    if (s.length == 2){
                        int loc = -1;
                        for (int i=0; i<tableName.size(); i++) {
                            if (s[0].toLowerCase().compareTo(tableName.get(i).getText().toLowerCase()) == 0) {
                                loc = i;
                                break;
                            }
                        }
                        ArrayList<Column> allCol = queryTables.get(loc).columns;
                        loc = -1;
                        for (int i=0; i<allCol.size(); i++){
                            if (s[1].toLowerCase().compareTo(allCol.get(i).getName())==0){
                                loc = i;
                                break;
                            }
                        }
                        if (loc==-1) {
                            return new QueryResult("Delete Failed, Unknown Key");
                        } else {
                            switch (allCol.get(loc).getType()){
                                case INT:
                                    attrValue = Integer.valueOf(attrValueOrigin);
                                    break;
                                case DOUBLE:
                                    attrValue = Double.valueOf(attrValueOrigin);
                                    break;
                                case FLOAT:
                                    attrValue = Float.valueOf(attrValueOrigin);
                                    break;
                                case LONG:
                                    attrValue = Long.valueOf(attrValueOrigin);
                                    break;
                                case STRING:
                                default:
                                    String text = attrValueOrigin.substring(1, attrValueOrigin.length() - 1);
                                    int maxLength = allCol.get(loc).getMaxLength();
                                    if (text.length() > maxLength)
                                        return new QueryResult(String.format("Insert Error: String Length Out of Bound for Column %s", allCol.get(loc).getName()));
                                    attrValue = Global.resize(text, maxLength);
                                    break;
                            }
                        }
                    } else {
                        ArrayList<Column> allCol = queryTables.get(0).columns;
                        int loc = -1;
                        for (int i=0; i<allCol.size(); i++){
                            if (attrName.compareTo(allCol.get(i).getName())==0){
                                loc = i;
                                break;
                            }
                        }
                        if (loc == -1) {
                            allCol = queryTables.get(1).columns;
                            for (int i = 0; i < allCol.size(); i++) {
                                if (attrName.compareTo(allCol.get(i).getName()) == 0) {
                                    loc = i;
                                    break;
                                }
                            }
                        }
                        if (loc == -1) {
                            return new QueryResult("Delete Failed, Unknown Key");
                        } else {
                            switch (allCol.get(loc).getType()){
                                case INT:
                                    attrValue = Integer.valueOf(attrValueOrigin);
                                    break;
                                case DOUBLE:
                                    attrValue = Double.valueOf(attrValueOrigin);
                                    break;
                                case FLOAT:
                                    attrValue = Float.valueOf(attrValueOrigin);
                                    break;
                                case LONG:
                                    attrValue = Long.valueOf(attrValueOrigin);
                                    break;
                                case STRING:
                                default:
                                    String text = attrValueOrigin.substring(1, attrValueOrigin.length() - 1);
                                    int maxLength = allCol.get(loc).getMaxLength();
                                    if (text.length() > maxLength)
                                        return new QueryResult(String.format("Insert Error: String Length Out of Bound for Column %s", allCol.get(loc).getName()));
                                    attrValue = Global.resize(text, maxLength);
                                    break;
                            }
                        }
                    }
                    ComparisonType op = getComparisonType(ctx.multiple_condition().condition().getChild(1).getText());
                    if (op == ComparisonType.UNDECODE){
                        return new QueryResult("Select Failed, Unsupported comparison type");
                    }
                    rowsToSelect = mQuery.queryVal(attrName, op, attrValue);
                } else {
                    rowsToSelect = mQuery.queryAll();
                }

                //拼接select结果
                StringJoiner header = new StringJoiner(", ");
                mQuery.getAttrNames().forEach(header::add);
                StringJoiner result= new StringJoiner("\n");
                result.add(header.toString());
                rowsToSelect.forEach(it -> {
                    result.add(it.toString());
                });
                result.add(String.format("Select totally %d row(s) successfully", rowsToSelect.size()));
                return new QueryResult(result.toString());

            } catch (Exception e){
                return new QueryResult("Select Failed: " + e.getMessage());
            }


        } else {
            //单表select语句处理
            String tableName = ctx.table_query().get(0).getText().toLowerCase();
            if (this.db.tableInDB(tableName))
            {
                TableP table;
                try{
                    table = this.db.getTable(tableName);
                    QueryResult mQuery;
                    ArrayList<Row> rowsToSelect;

                    ArrayList<String> columnNames = new ArrayList<String>();
                    ctx.result_column().forEach(it -> {
                        columnNames.add(it.getText().toLowerCase());
                    });

                    if (columnNames.get(0).compareTo("*") == 0){
                        mQuery = new QueryResult(table, null, true);
                    } else {
                        mQuery = new QueryResult(table, columnNames, false);
                    }

                    if (ctx.K_WHERE()!=null){
                        String attrName = ctx.multiple_condition().condition().getChild(0).getText().toLowerCase();
                        Object attrValue;
                        String attrValueOrigin = ctx.multiple_condition().condition().getChild(2).getText();
                        ArrayList<Column> allCol = table.columns;
                        int loc = -1;
                        for (int i=0; i<allCol.size(); i++){
                            if (attrName.compareTo(allCol.get(i).getName())==0){
                                loc = i;
                                break;
                            }
                        }
                        if (loc == -1){
                            return new QueryResult("Select Failed, Unknown Key");
                        } else {
                            switch (allCol.get(loc).getType()){
                                case INT:
                                    attrValue = Integer.valueOf(attrValueOrigin);
                                    break;
                                case DOUBLE:
                                    attrValue = Double.valueOf(attrValueOrigin);
                                    break;
                                case FLOAT:
                                    attrValue = Float.valueOf(attrValueOrigin);
                                    break;
                                case LONG:
                                    attrValue = Long.valueOf(attrValueOrigin);
                                    break;
                                case STRING:
                                default:
                                    String text = attrValueOrigin.substring(1, attrValueOrigin.length() - 1);
                                    int maxLength = allCol.get(loc).getMaxLength();
                                    if (text.length() > maxLength)
                                        return new QueryResult(String.format("Insert Error: String Length Out of Bound for Column %s", allCol.get(loc).getName()));
                                    attrValue = Global.resize(text, maxLength);
                                    break;
                            }
                        }
                        ComparisonType op = getComparisonType(ctx.multiple_condition().condition().getChild(1).getText());
                        if (op == ComparisonType.UNDECODE){
                            return new QueryResult("Select Failed, Unsupported comparison type");
                        }
                        rowsToSelect = mQuery.queryVal_s(attrName, op, attrValue);
                    } else {
                        rowsToSelect = mQuery.queryAll_s();
                    }
                    
                    //拼接select结果
                    StringJoiner header = new StringJoiner(", ");
                    mQuery.getAttrNames_s().forEach(header::add);
                    StringJoiner result= new StringJoiner("\n");
                    result.add(header.toString());
                    rowsToSelect.forEach(it -> {
                        result.add(it.toString());
                    });
                    result.add(String.format("Select totally %d row(s) successfully", rowsToSelect.size()));
                    return new QueryResult(result.toString());
                }
                catch (Exception e){
                    return new QueryResult("Select Failed: " + e.getMessage());
                }
            }
            else
            {
                return new QueryResult(String.format("Select Failed, Unknown table name: %s", tableName));
            }
        }
    }

    @Override
    public QueryResult visitCreate_view_stmt(SQLParser.Create_view_stmtContext ctx) {
        return null;
    }

    @Override
    public QueryResult visitDrop_view_stmt(SQLParser.Drop_view_stmtContext ctx) {
        return null;
    }

    @Override
    public QueryResult visitUpdate_stmt(SQLParser.Update_stmtContext ctx) {
        String tableName = ctx.table_name().getText().toLowerCase();
        if (this.db.tableInDB(tableName))
        {
            TableP table;
            try{
                table = this.db.getTable(tableName);
                if (table.currentSessionID != this.sessionID){
                    if (table.currentSessionID == -1){
                        table.currentSessionID = this.sessionID;
                    } else {
                        return new QueryResult("Update Failed, current table has been locked");
                    }
                }

                ArrayList<Row> rowsToUpdate;
                QueryResult mQuery = new QueryResult(table, null, true);
                String attrName = ctx.column_name().getText().toLowerCase();
                Object attrValue;
                String attrValueOrigin = ctx.expression().getText();
                ArrayList<Column> allCol = table.columns;
                int loc = -1;
                for (int i=0; i<allCol.size(); i++){
                    if (attrName.compareTo(allCol.get(i).getName())==0){
                        loc = i;
                        break;
                    }
                }
                if (loc == -1){
                    return new QueryResult("Delete Failed, Unknown Key");
                } else {
                    switch (allCol.get(loc).getType()){
                        case INT:
                            attrValue = Integer.valueOf(attrValueOrigin);
                            break;
                        case DOUBLE:
                            attrValue = Double.valueOf(attrValueOrigin);
                            break;
                        case FLOAT:
                            attrValue = Float.valueOf(attrValueOrigin);
                            break;
                        case LONG:
                            attrValue = Long.valueOf(attrValueOrigin);
                            break;
                        case STRING:
                        default:
                            String text = attrValueOrigin.substring(1, attrValueOrigin.length() - 1);
                            int maxLength = allCol.get(loc).getMaxLength();
                            if (text.length() > maxLength)
                                return new QueryResult(String.format("Insert Error: String Length Out of Bound for Column %s", allCol.get(loc).getName()));
                            attrValue = Global.resize(text, maxLength);
                            break;
                    }
                }
                if (ctx.K_WHERE()!=null){
                    String queryName = ctx.multiple_condition().condition().getChild(0).getText().toLowerCase();
                    Object queryValue;
                    String queryValueOrigin = ctx.multiple_condition().condition().getChild(2).getText();
                    loc = -1;
                    for (int i=0; i<allCol.size(); i++){
                        if (queryName.compareTo(allCol.get(i).getName())==0){
                            loc = i;
                            break;
                        }
                    }
                    if (loc == -1){
                        return new QueryResult("Update Failed, Unknown Key");
                    } else {
                        switch (allCol.get(loc).getType()){
                            case INT:
                                queryValue = Integer.valueOf(queryValueOrigin);
                                break;
                            case DOUBLE:
                                queryValue = Double.valueOf(queryValueOrigin);
                                break;
                            case FLOAT:
                                queryValue = Float.valueOf(queryValueOrigin);
                                break;
                            case LONG:
                                queryValue = Long.valueOf(queryValueOrigin);
                                break;
                            case STRING:
                            default:
                                String text = queryValueOrigin.substring(1, queryValueOrigin.length() - 1);
                                int maxLength = allCol.get(loc).getMaxLength();
                                if (text.length() > maxLength)
                                    return new QueryResult(String.format("Insert Error: String Length Out of Bound for Column %s", allCol.get(loc).getName()));
                                queryValue = Global.resize(text, maxLength);
                                break;
                        }
                    }
                    ComparisonType op = getComparisonType(ctx.multiple_condition().condition().getChild(1).getText());
                    if (op == ComparisonType.UNDECODE){
                        return new QueryResult("Update Failed, Unsupported comparison type");
                    }
                    rowsToUpdate = mQuery.queryVal_s(queryName, op, queryValue);
                } else {
                    rowsToUpdate = mQuery.queryAll_s();
                }

                if (!rowsToUpdate.isEmpty()) {
                    table.update(rowsToUpdate, attrName, attrValue);
                    if (this.db.txManager.getTransactionState(this.sessionID)) {
                        table.rowsForActions.push(rowsToUpdate);
                        table.actionType.push(Global.STATE_TYPE.UPDATE);
                        this.db.walManager.addStatement(this.db.getCurrentStatement());
                    } else {
                        //table.persist();
                        this.db.walManager.persist(this.db.getCurrentStatement());
                        table.currentSessionID = -1;
                    }
                    return new QueryResult(String.format("Update %d row(s) successfully", rowsToUpdate.size()));
                } else {
                    return new QueryResult("Update Failed, No such conditions");
                }


            }
            catch (Exception e){
                return new QueryResult("Update Failed: " + e.getMessage());
            }
        }
        else
        {
            return new QueryResult(String.format("Update Failed, Unknown table name: %s", tableName));
        }
    }

    @Override
    public QueryResult visitBegin_transaction_stmt(SQLParser.Begin_transaction_stmtContext ctx) {
        this.db.txManager.beginTransaction(this.sessionID);
        return new QueryResult("Start Transaction");
    }

    @Override
    public QueryResult visitCommit_stmt(SQLParser.Commit_stmtContext ctx){
        this.db.txManager.commitTransaction(this.sessionID);
        try {
            this.db.walManager.persist();
        } catch (Exception e) {
            return new QueryResult("Commit failed, " + e.getMessage());
        }
        return new QueryResult("Commit succeed");
    }

    @Override
    public QueryResult visitRollback_stmt(SQLParser.Rollback_stmtContext ctx){
        this.db.txManager.rollbackTransaction(this.sessionID);
        this.db.walManager.rollBackClearStatement();
        return new QueryResult("Rollback succeed");
    }

    @Override
    public QueryResult visitCheckpoint_stmt(SQLParser.Checkpoint_stmtContext ctx){
        //TODO
        try {
            this.db.txManager.persistTable(this.sessionID);
            this.db.walManager.clearLog();
        } catch (Exception e){
            return new QueryResult("Persist Failed: " + e.getMessage());
        }

        return new QueryResult("Persist succeed");
    }

    @Override
    public QueryResult visitColumn_def(SQLParser.Column_defContext ctx) {
        return null;
    }

    @Override
    public QueryResult visitType_name(SQLParser.Type_nameContext ctx) {
        return null;
    }

    @Override
    public QueryResult visitColumn_constraint(SQLParser.Column_constraintContext ctx) {
        return null;
    }

    @Override
    public QueryResult visitMultiple_condition(SQLParser.Multiple_conditionContext ctx) {
        return null;
    }

    @Override
    public QueryResult visitCondition(SQLParser.ConditionContext ctx) {
        return null;
    }

    @Override
    public QueryResult visitComparer(SQLParser.ComparerContext ctx) {
        return null;
    }

    @Override
    public QueryResult visitComparator(SQLParser.ComparatorContext ctx) {
        return null;
    }

    @Override
    public QueryResult visitExpression(SQLParser.ExpressionContext ctx) {
        return null;
    }

    @Override
    public QueryResult visitTable_constraint(SQLParser.Table_constraintContext ctx) {
        return null;
    }

    @Override
    public QueryResult visitResult_column(SQLParser.Result_columnContext ctx) {
        return null;
    }

    @Override
    public QueryResult visitTable_query(SQLParser.Table_queryContext ctx) {
        return null;
    }

    @Override
    public QueryResult visitAuth_level(SQLParser.Auth_levelContext ctx) {
        return null;
    }

    @Override
    public QueryResult visitLiteral_value(SQLParser.Literal_valueContext ctx) {
        return null;
    }

    @Override
    public QueryResult visitColumn_full_name(SQLParser.Column_full_nameContext ctx) {
        return null;
    }

    @Override
    public QueryResult visitDatabase_name(SQLParser.Database_nameContext ctx) {
        return null;
    }

    @Override
    public QueryResult visitTable_name(SQLParser.Table_nameContext ctx) {
        return null;
    }

    @Override
    public QueryResult visitUser_name(SQLParser.User_nameContext ctx) {
        return null;
    }

    @Override
    public QueryResult visitColumn_name(SQLParser.Column_nameContext ctx) {
        return null;
    }

    @Override
    public QueryResult visitView_name(SQLParser.View_nameContext ctx) {
        return null;
    }

//    @Override
//    public QueryResult visitPassword(SQLParser.PasswordContext ctx) {
//        return null;
//    }
//
//    @Override
//    public QueryResult visit(ParseTree parseTree) {
//        return null;
//    }
//
//    @Override
//    public QueryResult visitChildren(RuleNode ruleNode) {
//        return null;
//    }
//
//    @Override
//    public QueryResult visitTerminal(TerminalNode terminalNode) {
//        return null;
//    }
//
//    @Override
//    public QueryResult visitErrorNode(ErrorNode errorNode) {
//        return null;
//    }
}
