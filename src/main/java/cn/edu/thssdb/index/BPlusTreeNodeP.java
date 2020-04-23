package cn.edu.thssdb.index;

import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.utils.Global;
import javafx.util.Pair;
import org.omg.CORBA.INTERNAL;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

abstract class BPlusTreeNodeP{

	ArrayList<Entry> keys;

	// pageId(4)|prev page(4)|next page(4)|node type(4)|node size(4)
	int pageId;
	int prevPage;
	int nextPage;
	int nodeType;
	int nodeSize;

	BPlusTreeInfo info;

	abstract Row get(Entry key) throws IOException;

	abstract void put(Entry key, Row value) throws IOException;

	abstract void remove(Entry key) throws IOException;

	abstract boolean containsKey(Entry key) throws IOException;

	abstract Entry getFirstLeafKey() throws IOException;

	abstract Pair<Integer, Entry> split() throws IOException;

	abstract void merge(BPlusTreeNodeP sibling) throws IOException;

	int size() {
		return nodeSize;
	}


	abstract boolean isOverFlow();


	abstract boolean isUnderFlow();

	int binarySearch(Entry key) {
		return Collections.binarySearch(keys.subList(0, nodeSize), key);
	}

	void keysAdd(int index, Entry key) {
		for (int i = nodeSize; i > index; i--) {
			keys.set(i, keys.get(i - 1));
		}
		keys.set(index, key);
		nodeSize++;
	}

	void keysRemove(int index) {
		for (int i = index; i < nodeSize - 1; i++) {
			keys.set(i, keys.get(i + 1));
		}
		nodeSize--;
	}

	abstract void write() throws IOException;

	abstract void read() throws IOException;
}
