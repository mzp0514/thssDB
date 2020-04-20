package cn.edu.thssdb.schema;


import cn.edu.thssdb.type.ColumnType;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TableTest {

	private Table table;

	@Before
	public void setUp() throws IOException {
		Column[] columns = new Column[5];
//		String name, ColumnType type, int primary, boolean notNull, int maxLength
		columns[0] = new Column("id", ColumnType.INT, 1, true , 0);
		columns[1] = new Column("name", ColumnType.STRING, 0, true , 10);
		columns[2] = new Column("class", ColumnType.STRING, 0, true , 10);
		columns[3] = new Column("height", ColumnType.FLOAT, 0, true , 0);
		columns[4] = new Column("weight", ColumnType.DOUBLE, 0, true , 0);
		this.table = new Table("dbtest", "tb", columns);


	}

	@Test
	public void create() throws IOException {
		Column[] columns = new Column[5];
//		String name, ColumnType type, int primary, boolean notNull, int maxLength
		columns[0] = new Column("id", ColumnType.INT, 1, true , 0);
		columns[1] = new Column("name", ColumnType.STRING, 0, true , 10);
		columns[2] = new Column("class", ColumnType.STRING, 0, true , 10);
		columns[3] = new Column("height", ColumnType.FLOAT, 0, true , 0);
		columns[4] = new Column("weight", ColumnType.DOUBLE, 0, true , 0);
		this.table = new Table("dbtest", "tb", columns);
	}

	@Test
	public void resume() throws IOException, ClassNotFoundException {

		Table table2 = new Table("dbtest", "tb");
	}

	@Test
	public void testInsert() throws IOException {
		Entry[] entries = new Entry[5];
		entries[0] = new Entry(0);
		entries[1] = new Entry("mzp");
		entries[2] = new Entry("软件73");
		entries[3] = new Entry("200.0");
		entries[4] = new Entry("200.0");
		Row row = new Row(entries);
		table.insert(row);
	}




}


