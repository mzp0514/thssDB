package cn.edu.thssdb.index;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.type.BPlusNodeType;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.Global;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BPlusTreePTest {
	private BPlusTreeP tree;
	private ArrayList<Entry> keys;
	private ArrayList<Row> values;
	private HashMap<Entry, Row> map;

	@Before
	public void setUp() throws IOException {
		Column[] columns = new Column[5];
//		String name, ColumnType type, int primary, boolean notNull, int maxLength
		columns[0] = new Column("id", ColumnType.INT, 1, true, 0);
		columns[1] = new Column("name", ColumnType.STRING, 0, true, 10);
		columns[2] = new Column("class", ColumnType.STRING, 0, true, 10);
		columns[3] = new Column("height", ColumnType.FLOAT, 0, true, 0);
		columns[4] = new Column("weight", ColumnType.DOUBLE, 0, true, 0);

		tree = new BPlusTreeP("data/dbtest/index.data", 0, columns);
		keys = new ArrayList<>();
		values = new ArrayList<>();
		map = new HashMap<>();

		Entry[] entries = new Entry[5];
		entries[0] = new Entry(0);
		entries[1] = new Entry(Global.resize("mzp", 10));
		entries[2] = new Entry(Global.resize("73", 10));

		for (int i = 0; i < 200; i++) {
			entries[0] = new Entry(i);
			entries[3] = new Entry((float) i);
			entries[4] = new Entry((double) i);
			Row row = new Row(entries);
			keys.add(entries[0]);
			values.add(row);
			map.put(entries[0], row);
			tree.put(entries[0], row);
		}

	}

	private void printTree(BPlusTreeP tree) throws IOException {
		for(int i = 0; i < tree.info.pageNum; i++){
			if(tree.info.freePages.contains(i)){
				continue;
			}
			int type = tree.info.readNodeType(i);
			BPlusTreeNodeP child;
			if(type == BPlusNodeType.LEAF.ordinal()){
				child = new BPlusTreeLeafNodeP(i, tree.info);
				System.out.println("type:" + child.nodeType);
				System.out.println("id:" + child.pageId);
				System.out.println(child.keys.subList(0, child.nodeSize).toString());
				System.out.println("size:" + child.nodeSize);
				System.out.println("next:" + child.nextPage + "\n");
			}
			else{
//
//				child = new BPlusTreeInternalNodeP(i, tree.info);
//				System.out.println("type:" + child.nodeType);
//				System.out.println("id:" + child.pageId);
//				System.out.println(child.keys.subList(0, child.nodeSize).toString());
//				System.out.println(((BPlusTreeInternalNodeP) child).children.toString());
//				System.out.println("size:" + child.nodeSize + "\n");
			}
		}
	}

	@Test
	public void testRecover() throws IOException, ClassNotFoundException {
		BPlusTreeP tree2 = new BPlusTreeP("data/dbtest/index.data");

		for (Entry key : keys)
			assertEquals(map.get(key).toString(), tree2.get(key).toString());//比较所有的get结果是否一样
	}
//
	@Test
	public void testRemove() throws IOException {
		int size = keys.size();
		for (int i = 0; i < size; i += 2)
			tree.remove(keys.get(i));//remove掉一半的数据
		assertEquals(size / 2, tree.size());//比较size是否等于原来的一半
		for (int i = 1; i < size; i += 2)
			assertEquals(map.get(keys.get(i)).toString(), tree.get(keys.get(i)).toString());//删除一半后再比较另一半的get结果是否一样
	}

//
//	@Test
//	public void testIterator() {
//		BPlusTreeIterator<Integer, Integer> iterator = tree.iterator();
//		int c = 0;
//		while (iterator.hasNext()) {
//			assertTrue(values.contains(iterator.next().getValue()));
//			c++;
//		}
//		assertEquals(values.size(), c);
//	}
}
