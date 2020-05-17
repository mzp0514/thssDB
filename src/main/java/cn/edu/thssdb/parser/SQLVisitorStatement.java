package cn.edu.thssdb.parser;

import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.type.ColumnType;
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
        return null;
    }

    @Override
    public QueryResult visitDrop_db_stmt(SQLParser.Drop_db_stmtContext ctx) {
        return null;
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
        return null;
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
        return null;
    }

    @Override
    public QueryResult visitDelete_stmt(SQLParser.Delete_stmtContext ctx) {
        return null;
    }

    @Override
    public QueryResult visitDrop_table_stmt(SQLParser.Drop_table_stmtContext ctx) {
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
                                    entries[i] = new Entry(text);
                                    break;
                                default:
                                    break;
                            }
                        }
                    }
                    Row r = new Row(entries);
                    table.insert(r);
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
        return null;
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
        return null;
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
