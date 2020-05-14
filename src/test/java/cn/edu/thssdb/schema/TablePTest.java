package cn.edu.thssdb.schema;


import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.type.ComparisonType;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TablePTest {

	private TableP table;

	Column[] columns;

	private ArrayList<Row> rows;

	@Before
	public void setUp() throws IOException {
		columns = new Column[5];
//		String name, ColumnType type, int primary, boolean notNull, int maxLength
		columns[0] = new Column("id", ColumnType.INT, 1, true , 0);
		columns[1] = new Column("name", ColumnType.STRING, 0, true , 3);
		columns[2] = new Column("class", ColumnType.STRING, 0, true , 2);
		columns[3] = new Column("height", ColumnType.DOUBLE, 0, true , 0);
		columns[4] = new Column("weight", ColumnType.DOUBLE, 0, true , 0);

		this.rows = new ArrayList<>();
		Entry[] entries = new Entry[5];
		entries[0] = new Entry(0);
		entries[1] = new Entry("mzp");
		entries[2] = new Entry("73");

		for(int i = 0; i < 1000; i++){
			entries[0] = new Entry(i);
			entries[3] = new Entry((double)i);
			entries[4] = new Entry((double)i);
			Row row = new Row(entries);
			rows.add(row);
		}
	}

	@Test
	public void insertTest() throws IOException {
		this.table = new TableP("dbtest", "tb", columns);
		Entry[] entries = new Entry[5];
		entries[0] = new Entry(0);
		entries[1] = new Entry("mzp");
		entries[2] = new Entry("73");

		for(int i = 0; i < 1000; i++){
			entries[0] = new Entry(i);
			entries[3] = new Entry((double)i);
			entries[4] = new Entry((double)i);
			Row row = new Row(entries);
			table.insert(row);
		}
		table.close();
	}

	@Test
	public void selectTest() throws IOException, ClassNotFoundException {
		TableP table2 = new TableP("dbtest", "tb");
		ArrayList<Row> res1 = table2.select(0, 100, ComparisonType.EQUAL);

		ArrayList<Row> res2 = new ArrayList<>();
		for(Row r: rows){
			if(r.getEntries().get(0).equals(new Entry(100))){
				res2.add(r);
			}
		}

		assertEquals(res2.toString(), res1.toString());
		table2.close();
	}

	@Test
	public void selectTest2() throws IOException, ClassNotFoundException {
		TableP table2 = new TableP("dbtest", "tb");
		ArrayList<Row> res1 = table2.select(0, 100, ComparisonType.NEQUAL);

		ArrayList<Row> res2 = new ArrayList<>();
		for(Row r: rows){
			if(!r.getEntries().get(0).equals(new Entry(100))){
				res2.add(r);
			}
		}

		assertEquals(res2.size(), res1.size());
		table2.close();
	}

	@Test
	public void selectTest3() throws IOException, ClassNotFoundException {
		TableP table2 = new TableP("dbtest", "tb");
		ArrayList<Row> res1 = table2.select(0, 200, ComparisonType.GREATER);

		ArrayList<Row> res2 = new ArrayList<>();
		for(Row r: rows){
			if(r.getEntries().get(0).compareTo(new Entry(200)) == 1){
				res2.add(r);
			}
		}

		assertEquals(res2.toString(), res1.toString());
		table2.close();
	}

	@Test
	public void selectTest4() throws IOException, ClassNotFoundException {
		TableP table2 = new TableP("dbtest", "tb");
		ArrayList<Row> res1 = table2.select(0, 50, ComparisonType.LESS);

		ArrayList<Row> res2 = new ArrayList<>();
		for(Row r: rows){
			if(r.getEntries().get(0).compareTo(new Entry(50)) == -1){
				res2.add(r);
			}
		}

		assertEquals(res2.toString(), res1.toString());
		table2.close();
	}

	@Test
	public void selectTest5() throws IOException, ClassNotFoundException {
		TableP table2 = new TableP("dbtest", "tb");
		ArrayList<Row> res1 = table2.select(0, 50, ComparisonType.NLESS);

		ArrayList<Row> res2 = new ArrayList<>();
		for(Row r: rows){
			if(r.getEntries().get(0).compareTo(new Entry(50)) != -1){
				res2.add(r);
			}
		}

		assertEquals(res2.toString(), res1.toString());
		table2.close();
	}

	@Test
	public void selectTest6() throws IOException, ClassNotFoundException {
		TableP table2 = new TableP("dbtest", "tb");
		ArrayList<Row> res1 = table2.select(0, 50, ComparisonType.NGREATER);

		ArrayList<Row> res2 = new ArrayList<>();
		for(Row r: rows){
			if(r.getEntries().get(0).compareTo(new Entry(50)) != 1){
				res2.add(r);
			}
		}

		assertEquals(res2.toString(), res1.toString());
		table2.close();
	}

	@Test
	public void selectTest7() throws IOException, ClassNotFoundException {
		TableP table2 = new TableP("dbtest", "tb");
		ArrayList<Row> res1 = table2.select(1, "mzpp", ComparisonType.NGREATER);

		ArrayList<Row> res2 = new ArrayList<>();
		for(Row r: rows){
			if(r.getEntries().get(1).compareTo(new Entry("hgd")) != 1){
				res2.add(r);
			}
		}

		assertEquals(res2.toString(), res1.toString());
		table2.close();
	}

	@Test
	public void selectTest8() throws IOException, ClassNotFoundException {
		TableP table2 = new TableP("dbtest", "tb");
		ArrayList<Row> res1 = table2.select(3, 100.0, ComparisonType.NGREATER);

		ArrayList<Row> res2 = new ArrayList<>();
		for(Row r: rows){
			if(r.getEntries().get(3).compareTo(new Entry(100.0)) != 1){
				res2.add(r);
			}
		}

		assertEquals(res2.toString(), res1.toString());
		table2.close();
	}

	@Test
	public void selectTest9() throws IOException, ClassNotFoundException {
		TableP table2 = new TableP("dbtest", "tb");
		ArrayList<Row> res1 = table2.select(3, 100.0, ComparisonType.EQUAL);

		ArrayList<Row> res2 = new ArrayList<>();
		for(Row r: rows){
			if(r.getEntries().get(3).equals(new Entry(100.0))){
				res2.add(r);
			}
		}

		assertEquals(res2.toString(), res1.toString());
		table2.close();
	}

	@Test
	public void selectTest10() throws IOException, ClassNotFoundException {
		TableP table2 = new TableP("dbtest", "tb");
		ArrayList<Row> res1 = table2.select(3, 100.0, ComparisonType.NEQUAL);

		ArrayList<Row> res2 = new ArrayList<>();
		for(Row r: rows){
			if(!r.getEntries().get(3).equals(new Entry(100.0))){
				res2.add(r);
			}
		}

		assertEquals(res2.toString(), res1.toString());
		table2.close();
	}

	@Test
	public void selectTest11() throws IOException, ClassNotFoundException {
		TableP table2 = new TableP("dbtest", "tb");
		ArrayList<Row> res1 = table2.select(3, 100.0, ComparisonType.GREATER);

		ArrayList<Row> res2 = new ArrayList<>();
		for(Row r: rows){
			if(r.getEntries().get(3).compareTo(new Entry(100.0)) == 1){
				res2.add(r);
			}
		}

		assertEquals(res2.toString(), res1.toString());
		table2.close();
	}

	@Test
	public void selectTest12() throws IOException, ClassNotFoundException {
		TableP table2 = new TableP("dbtest", "tb");
		ArrayList<Row> res1 = table2.select(3, 100.0, ComparisonType.LESS);

		ArrayList<Row> res2 = new ArrayList<>();
		for(Row r: rows){
			if(r.getEntries().get(3).compareTo(new Entry(100.0)) == -1){
				res2.add(r);
			}
		}

		assertEquals(res2.toString(), res1.toString());
		table2.close();
	}

	@Test
	public void selectTest13() throws IOException, ClassNotFoundException {
		TableP table2 = new TableP("dbtest", "tb");
		ArrayList<Row> res1 = table2.select(3, 100.0, ComparisonType.NLESS);

		ArrayList<Row> res2 = new ArrayList<>();
		for(Row r: rows){
			if(r.getEntries().get(3).compareTo(new Entry(100.0)) != -1){
				res2.add(r);
			}
		}

		assertEquals(res2.toString(), res1.toString());
		table2.close();
	}
}
