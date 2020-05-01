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

	private ArrayList<Row> rows;

	@Before
	public void setUp() throws IOException {
		Column[] columns = new Column[5];
//		String name, ColumnType type, int primary, boolean notNull, int maxLength
		columns[0] = new Column("id", ColumnType.INT, 1, true , 0);
		columns[1] = new Column("name", ColumnType.STRING, 0, true , 3);
		columns[2] = new Column("class", ColumnType.STRING, 0, true , 2);
		columns[3] = new Column("height", ColumnType.DOUBLE, 0, true , 0);
		columns[4] = new Column("weight", ColumnType.DOUBLE, 0, true , 0);
	//	this.table = new TableP("dbtest", "tb", columns);

		this.rows = new ArrayList<>();
	}

	@Test
	public void resume() throws IOException, ClassNotFoundException {
		insertTest();
		TableP table2 = new TableP("dbtest", "tb");
		//assertEquals(table.index.size(),table2.index.size());
	}


	@Test
	public void insertTest() throws IOException {
		Entry[] entries = new Entry[5];
		entries[0] = new Entry(0);
		entries[1] = new Entry("mzp");
		entries[2] = new Entry("73");

		for(int i = 0; i < 1000; i++){
			entries[0] = new Entry(i);
			entries[3] = new Entry((double)i);
			entries[4] = new Entry((double)i);
			Row row = new Row(entries);
			//table.insert(row);
			rows.add(row);
		}

	}

	@Test
	public void deleteTest() throws IOException, ClassNotFoundException {
		insertTest();
		Entry[] entries = new Entry[5];
		entries[0] = new Entry(0);
		entries[1] = new Entry("mzp");
		entries[2] = new Entry("73");
		ArrayList<Row> rows = new ArrayList<>();
		for(int i = 0; i < 200; i++) {
			entries[0] = new Entry(i);
			entries[3] = new Entry((double) i);
			entries[4] = new Entry((double) i);
			Row row = new Row(entries);
			rows.add(row);
		}

		table.delete(rows);
		//assertEquals(800, table.index.size());
	}

	@Test
	public void selectTest() throws IOException, ClassNotFoundException {
		insertTest();
		TableP table2 = new TableP("dbtest", "tb");
		ArrayList<Row> res1 = table2.select("id", 100, ComparisonType.EQUAL);

		ArrayList<Row> res2 = new ArrayList<>();
		for(Row r: rows){
			if(r.getEntries().get(0).equals(new Entry(100))){
				res2.add(r);
			}
		}

		assertEquals(res2.toString(), res1.toString());
	}

	@Test
	public void selectTest2() throws IOException {
		insertTest();
		ArrayList<Row> res1 = table.select("id", 100, ComparisonType.NEQUAL);

		ArrayList<Row> res2 = new ArrayList<>();
		for(Row r: rows){
			if(!r.getEntries().get(0).equals(new Entry(100))){
				res2.add(r);
			}
		}

		assertEquals(res2.size(), res1.size());
	}

	@Test
	public void selectTest3() throws IOException, ClassNotFoundException {
		insertTest();
		TableP table2 = new TableP("dbtest", "tb");
		ArrayList<Row> res1 = table2.select("id", 200, ComparisonType.GREATER);

		ArrayList<Row> res2 = new ArrayList<>();
		for(Row r: rows){
			if(r.getEntries().get(0).compareTo(new Entry(200)) == 1){
				res2.add(r);
			}
		}

		assertEquals(res2.toString(), res1.toString());
	}

	@Test
	public void selectTest4() throws IOException {
		insertTest();
		ArrayList<Row> res1 = table.select("id", 50, ComparisonType.LESS);

		ArrayList<Row> res2 = new ArrayList<>();
		for(Row r: rows){
			if(r.getEntries().get(0).compareTo(new Entry(50)) == -1){
				res2.add(r);
			}
		}

		assertEquals(res2.toString(), res1.toString());
	}

	@Test
	public void selectTest5() throws IOException {
		insertTest();
		ArrayList<Row> res1 = table.select("id", 50, ComparisonType.NLESS);

		ArrayList<Row> res2 = new ArrayList<>();
		for(Row r: rows){
			if(r.getEntries().get(0).compareTo(new Entry(50)) != -1){
				res2.add(r);
			}
		}

		assertEquals(res2.toString(), res1.toString());
	}

	@Test
	public void selectTest6() throws IOException {
		insertTest();
		ArrayList<Row> res1 = table.select("id", 50, ComparisonType.NGREATER);

		ArrayList<Row> res2 = new ArrayList<>();
		for(Row r: rows){
			if(r.getEntries().get(0).compareTo(new Entry(50)) != 1){
				res2.add(r);
			}
		}

		assertEquals(res2.toString(), res1.toString());
	}

	@Test
	public void selectTest7() throws IOException {
		insertTest();
		ArrayList<Row> res1 = table.select("name", "mzpp", ComparisonType.NGREATER);

		ArrayList<Row> res2 = new ArrayList<>();
		for(Row r: rows){
			if(r.getEntries().get(1).compareTo(new Entry("hgd")) != 1){
				res2.add(r);
			}
		}

		assertEquals(res2.toString(), res1.toString());
	}

	@Test
	public void selectTest8() throws IOException {
		insertTest();
		ArrayList<Row> res1 = table.select("height", 100.0, ComparisonType.NGREATER);

		ArrayList<Row> res2 = new ArrayList<>();
		for(Row r: rows){
			if(r.getEntries().get(3).compareTo(new Entry(100.0)) != 1){
				res2.add(r);
			}
		}

		assertEquals(res2.toString(), res1.toString());
	}

	@Test
	public void selectTest9() throws IOException {
		insertTest();
		ArrayList<Row> res1 = table.select("height", 100.0, ComparisonType.EQUAL);

		ArrayList<Row> res2 = new ArrayList<>();
		for(Row r: rows){
			if(r.getEntries().get(3).equals(new Entry(100.0))){
				res2.add(r);
			}
		}

		assertEquals(res2, res1);
	}

	@Test
	public void selectTest10() throws IOException {
		insertTest();
		ArrayList<Row> res1 = table.select("height", 100.0, ComparisonType.NEQUAL);

		ArrayList<Row> res2 = new ArrayList<>();
		for(Row r: rows){
			if(!r.getEntries().get(3).equals(new Entry(100.0))){
				res2.add(r);
			}
		}

		assertEquals(res2, res1);
	}

	@Test
	public void selectTest11() throws IOException {
		insertTest();
		ArrayList<Row> res1 = table.select("height", 100.0, ComparisonType.GREATER);

		ArrayList<Row> res2 = new ArrayList<>();
		for(Row r: rows){
			if(r.getEntries().get(3).compareTo(new Entry(100.0)) == 1){
				res2.add(r);
			}
		}

		assertEquals(res2, res1);
	}

	@Test
	public void selectTest12() throws IOException {
		insertTest();
		ArrayList<Row> res1 = table.select("height", 100.0, ComparisonType.LESS);

		ArrayList<Row> res2 = new ArrayList<>();
		for(Row r: rows){
			if(r.getEntries().get(3).compareTo(new Entry(100.0)) == -1){
				res2.add(r);
			}
		}

		assertEquals(res2, res1);
	}

	@Test
	public void selectTest13() throws IOException {
		insertTest();
		ArrayList<Row> res1 = table.select("height", 100.0, ComparisonType.NLESS);

		ArrayList<Row> res2 = new ArrayList<>();
		for(Row r: rows){
			if(r.getEntries().get(3).compareTo(new Entry(100.0)) != -1){
				res2.add(r);
			}
		}

		assertEquals(res2, res1);
	}
}
