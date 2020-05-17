package cn.edu.thssdb.schema;

import cn.edu.thssdb.type.ColumnType;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

public class TablePTest2 {
    private TableP table;

    Column[] columns;

    private ArrayList<Row> rows;


    @Before
    public void setUp() throws IOException {
        columns = new Column[2];
//		String name, ColumnType type, int primary, boolean notNull, int maxLength
        columns[0] = new Column("name", ColumnType.STRING, 0, true , 3);
        columns[1] = new Column("id", ColumnType.INT, 1, true , 0);

    }

    @Test
    public void insertTest() throws IOException, ClassNotFoundException {
        this.table = new TableP("dbtest", "tb", columns);
        Entry[] entries = new Entry[2];
        entries[0] = new Entry("Bob");
        entries[1] = new Entry(15);
        Row row = new Row(entries);
        table.insert(row);

        entries[0] = new Entry("Bob");
        entries[1] = new Entry(17);
        row = new Row(entries);
        table.insert(row);

        entries[0] = new Entry("Bob");
        entries[1] = new Entry(19);
        row = new Row(entries);
        table.insert(row);

        table.close();
    }

    @Test
    public void insertTest2() throws IOException, ClassNotFoundException {
        this.table = new TableP("dbtest", "tb");
        Entry[] entries = new Entry[2];
        entries[0] = new Entry("Bob");
        entries[1] = new Entry(15);
        Row row = new Row(entries);
        table.insert(row);

        table.close();
    }


}
