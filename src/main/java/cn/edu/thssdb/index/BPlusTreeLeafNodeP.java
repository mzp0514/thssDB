package cn.edu.thssdb.index;

import cn.edu.thssdb.exception.DuplicateKeyException;
import cn.edu.thssdb.exception.KeyNotExistException;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.type.BPlusNodeType;
import cn.edu.thssdb.type.ColumnType;
import javafx.util.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class BPlusTreeLeafNodeP extends BPlusTreeNodeP{
	ArrayList<Row> values;

	private BPlusTreeLeafNodeP next;

	BPlusTreeLeafNodeP(BPlusTreeInfo info, int size) throws IOException {
		this.info = info;
		this.pageId = info.findFreePage();
		this.nodeType = BPlusNodeType.LEAF.ordinal();
		keys = new ArrayList<>(Collections.nCopies((int) (1.5 * info.leafDataMaxLen) + 1, null));
		values = new ArrayList<>(Collections.nCopies((int) (1.5 * info.leafDataMaxLen) + 1, null));
		write();
		nodeSize = size;
	}

	BPlusTreeLeafNodeP(int pageId, BPlusTreeInfo info) throws IOException {
		this.info = info;
		this.pageId = pageId;
		this.nodeType = BPlusNodeType.LEAF.ordinal();
		keys = new ArrayList<>(Collections.nCopies((int) (1.5 * info.leafDataMaxLen) + 1, null));
		values = new ArrayList<>(Collections.nCopies((int) (1.5 * info.leafDataMaxLen) + 1, null));
		read();
	}

	@Override
  boolean isOverFlow() {
		return nodeSize > info.leafDataMaxLen;
	}

	@Override
	boolean isUnderFlow() {
		return nodeSize < info.leafDataMaxLen / 2;
	}

	@Override
	void write() throws IOException {
		info.writeIndex(BPlusNodeType.LEAF.ordinal(), ColumnType.INT, pageId * info.pageSize, -1);
		info.writeIndex(prevPage, ColumnType.INT, -1, -1);
		info.writeIndex(nextPage, ColumnType.INT, -1, -1);
		info.writeIndex(nodeSize, ColumnType.INT, -1, -1);

		for(int i = 0; i < nodeSize; i++){
			info.writeIndex(keys.get(i).value, info.keyType, -1, info.columns[info.keyId].getMaxLength());
			ArrayList<Entry> entries = values.get(i).getEntries();
			for(int j = 0; j < info.columns.length; j++) {
				info.writeIndex(entries.get(j).value, info.columns[j].getType(), -1, info.columns[j].getMaxLength());
			}
		}
	}

	@Override
	void read() throws IOException {
		nodeType = (int) info.readIndex(ColumnType.INT, pageId * info.pageSize, -1);
		prevPage = (int) info.readIndex(ColumnType.INT, -1, -1);
		nextPage = (int) info.readIndex(ColumnType.INT, -1, -1);
		nodeSize = (int) info.readIndex(ColumnType.INT, -1, -1);

		for(int i = 0; i < nodeSize; i++){
			Object k = info.readIndex(info.keyType, -1, info.columns[info.keyId].getMaxLength());
			keys.set(i, new Entry((Comparable) k));
			int len = info.columns.length;
			Entry[] entries = new Entry[len];
			for(int j = 0; j < len; j++){
				Object tmp = info.readIndex(info.columns[j].getType(), -1, info.columns[j].getMaxLength());
				entries[j] = new Entry((Comparable) tmp);
			}
			values.set(i, new Row(entries));
		}
	}


	private void valuesAdd(int index, Row value) {
		for (int i = nodeSize; i > index; i--)
			values.set(i, values.get(i - 1));
		values.set(index, value);
	}

	private void valuesRemove(int index) {
		for (int i = index; i < nodeSize - 1; i++)
			values.set(i, values.get(i + 1));
	}

	boolean hasNext(){
		return nextPage != -1;
	}

	BPlusTreeNodeP next() throws IOException {
		return page2instance(nextPage);
	}

	@Override
	boolean containsKey(Entry key) {
		return binarySearch(key) >= 0;
	}

	@Override
	Row  get(Entry key) {
		int index = binarySearch(key);
		if (index >= 0)
			return values.get(index);
		throw new KeyNotExistException();
	}

	@Override
	void put(Entry key, Row  value) throws IOException {
		int index = binarySearch(key);
		int valueIndex = index >= 0 ? index : -index - 1;
		if (index >= 0)
			throw new DuplicateKeyException();
		else {
			valuesAdd(valueIndex, value);
			keysAdd(valueIndex, key);
			write();
		}

	}

	@Override
	void remove(Entry key) throws IOException {
		int index = binarySearch(key);
		if (index >= 0) {
			valuesRemove(index);
			keysRemove(index);
			write();
		} else
			throw new KeyNotExistException();
	}

	@Override
	Entry getFirstLeafKey() {
		return keys.get(0);
	}

	@Override
	Pair<Integer, Entry> split() throws IOException {
		int from = (size() + 1) / 2;
		int to = size();
		BPlusTreeLeafNodeP newSiblingNode = new BPlusTreeLeafNodeP(info, to - from);
		for (int i = 0; i < to - from; i++) {
			newSiblingNode.keys.set(i, keys.get(i + from));
			newSiblingNode.values.set(i, values.get(i + from));
			keys.set(i + from, null);
			values.set(i + from, null);
		}
		nodeSize = from;
		newSiblingNode.nextPage = nextPage;
		nextPage = newSiblingNode.pageId;
		newSiblingNode.write();
		return new Pair<>(newSiblingNode.pageId, newSiblingNode.getFirstLeafKey());
	}

	@Override
	void merge(BPlusTreeNodeP sibling) throws IOException {
		int index = size();
		BPlusTreeLeafNodeP node = (BPlusTreeLeafNodeP) sibling;
		int length = node.size();
		for (int i = 0; i < length; i++) {
			keys.set(i + index, node.keys.get(i));
			values.set(i + index, node.values.get(i));
		}
		nodeSize = index + length;
		nextPage = node.nextPage;
		info.freePages.add(node.pageId);
		info.write();
	}
}
