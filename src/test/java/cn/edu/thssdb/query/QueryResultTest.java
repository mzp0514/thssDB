package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.*;

import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.type.ComparisonType;
import cn.edu.thssdb.type.JoinType;
import javafx.scene.control.Tab;
import org.junit.Before;
import org.junit.Test;

import javax.xml.ws.RespectBinding;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;
public class QueryResultTest {

	Column[] columns;

	Column[] columns2;

	private ArrayList<Row> rows;

	@Before
	public void setUp() throws IOException {
		columns = new Column[5];
//		String name, ColumnType type, int primary, boolean notNull, int maxLength
		columns[0] = new Column("id", ColumnType.INT, 1, true, 0);
		columns[1] = new Column("name", ColumnType.STRING, 0, true, 3);
		columns[2] = new Column("class", ColumnType.STRING, 0, true, 2);
		columns[3] = new Column("height", ColumnType.DOUBLE, 0, true, 0);
		columns[4] = new Column("weight", ColumnType.DOUBLE, 0, true, 0);

		columns2 = new Column[5];
//		String name, ColumnType type, int primary, boolean notNull, int maxLength
		columns2[0] = new Column("id", ColumnType.INT, 1, true, 0);
		columns2[1] = new Column("name", ColumnType.STRING, 0, true, 3);
		columns2[2] = new Column("qqq", ColumnType.STRING, 0, true, 2);
		columns2[3] = new Column("www", ColumnType.DOUBLE, 0, true, 0);
		columns2[4] = new Column("eee", ColumnType.DOUBLE, 0, true, 0);

		TableP table = new TableP("dbtest", "tb", columns);
		TableP table2 = new TableP("dbtest", "tb2", columns2);
		Entry[] entries = new Entry[5];
		entries[0] = new Entry(0);
		entries[1] = new Entry("mzp");
		entries[2] = new Entry("73");

		for (int i = 0; i < 10; i++) {
			entries[0] = new Entry(i);
			entries[3] = new Entry((double) i);
			entries[4] = new Entry((double) i);
			Row row = new Row(entries);
			table.insert(row);
		}

		for (int i = 0; i < 10; i++) {
			entries[0] = new Entry(i + 5);
			entries[3] = new Entry((double) (i + 5));
			entries[4] = new Entry((double) (i + 5));
			Row row = new Row(entries);
			table2.insert(row);
		}
		table.close();
		table2.close();
	}



	@Test
	public void selectTest1() throws IOException, ClassNotFoundException {
		TableP table = new TableP("dbtest", "tb");
		TableP table2= new TableP("dbtest", "tb2");

		QueryResult qr = new QueryResult(Arrays.asList(table, table2),
				Arrays.asList("tb.id","tb.name","tb.weight","tb.height", "tb2.id", "tb2.name", "tb2.qqq", "tb2.www"),
				"", "", JoinType.NATURAL_JOIN, true);

		System.out.println(qr.getAttrNames().toString());
		ArrayList<Row> res = qr.queryVal("tb.id", ComparisonType.NEQUAL, 9);
		for(int i = 0; i < res.size(); i++){
			System.out.println(res.get(i).toString());
		}
		table.close();
		table2.close();
	}

	@Test
	public void selectTest2() throws IOException, ClassNotFoundException {
		TableP table = new TableP("dbtest", "tb");
		TableP table2= new TableP("dbtest", "tb2");

		QueryResult qr = new QueryResult(Arrays.asList(table, table2),
				Arrays.asList("tb.id","tb.name","tb.weight","tb.height","tb2.qqq", "tb2.www"),
				"tb.name", "tb2.name", JoinType.INNER_JOIN, true);

		System.out.println(qr.getAttrNames().toString());
		ArrayList<Row> res = qr.queryVal("tb.name", ComparisonType.NEQUAL, 9);
		for(int i = 0; i < res.size(); i++){
			System.out.println(res.get(i).toString());
		}
		table.close();
		table2.close();
	}

	@Test
	public void selectTest3() throws IOException, ClassNotFoundException {
		TableP table = new TableP("dbtest", "tb");
		TableP table2= new TableP("dbtest", "tb2");

		QueryResult qr = new QueryResult(Arrays.asList(table, table2),
				Arrays.asList("tb.id","tb.name","tb.weight","tb.height","tb2.qqq", "tb2.www"),
				"tb.name", "tb2.name", JoinType.INNER_JOIN, true);

		System.out.println(qr.getAttrNames().toString());
		ArrayList<Row> res = qr.queryAll();
		for(int i = 0; i < res.size(); i++){
			System.out.println(res.get(i).toString());
		}
		table.close();
		table2.close();
	}

	@Test
	public void selectTest4() throws IOException, ClassNotFoundException {
		TableP table = new TableP("dbtest", "tb");
		TableP table2= new TableP("dbtest", "tb2");

		QueryResult qr = new QueryResult(Arrays.asList(table, table2),
				Arrays.asList("tb.id","tb.name","tb.weight","tb.height","tb2.qqq", "tb2.www"),
				"tb.name", "tb2.name", JoinType.NATURAL_JOIN, true);

		System.out.println(qr.getAttrNames().toString());
		ArrayList<Row> res = qr.queryAll();
		for(int i = 0; i < res.size(); i++){
			System.out.println(res.get(i).toString());
		}
		table.close();
		table2.close();
	}

	@Test
	public void selectTest5() throws IOException, ClassNotFoundException {
		TableP table = new TableP("dbtest", "tb");

		QueryResult qr = new QueryResult(table, null, true);

		System.out.println(qr.getAttrNames_s().toString());
		ArrayList<Row> res = qr.queryAll_s();
		for(int i = 0; i < res.size(); i++){
			System.out.println(res.get(i).toString());
		}
		table.close();
	}

	@Test
	public void selectTest6() throws IOException, ClassNotFoundException {
		TableP table = new TableP("dbtest", "tb");

		QueryResult qr = new QueryResult(table, Arrays.asList("id", "name"), false);

		System.out.println(qr.getAttrNames_s().toString());
		ArrayList<Row> res = qr.queryVal_s("id", ComparisonType.LESS, 5);
		for(int i = 0; i < res.size(); i++){
			System.out.println(res.get(i).toString());
		}
		table.close();
	}


}
