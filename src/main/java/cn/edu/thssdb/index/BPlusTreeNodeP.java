package cn.edu.thssdb.index;

import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.type.BPlusNodeType;
import javafx.util.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

abstract class BPlusTreeNodeP{

	ArrayList<Entry> keys;
	int pageId;
	int prevPage;
	int nextPage = -1;
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

	BPlusTreeNodeP page2instance(int page) throws IOException {
		int type = info.readNodeType(page);
		BPlusTreeNodeP child;
		if(type == BPlusNodeType.LEAF.ordinal()){
			child = new BPlusTreeLeafNodeP(page, info);
		}
		else{
			child = new BPlusTreeInternalNodeP(page, info);
		}
		return child;
	}

	abstract void write() throws IOException;

	abstract void read() throws IOException;
}
