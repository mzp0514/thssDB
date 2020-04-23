package cn.edu.thssdb.index;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.type.BPlusNodeType;
import javafx.util.Pair;

import java.io.IOException;

//public final class BPlusTreeP implements Iterable<Pair<Entry, Row>> {
public final class BPlusTreeP {
	BPlusTreeNodeP root;
	BPlusTreeInfo info;
	int size;
	// node type(4)|prev page(4)|next page(4)|node size(4)

	public BPlusTreeP(String filename, int keyId, Column[] columns) throws IOException {
		info = new BPlusTreeInfo(filename, keyId, columns, columns[keyId].getMaxLength());
		root = new BPlusTreeLeafNodeP(info, 0);
	}

	public BPlusTreeP(String filename) throws IOException, ClassNotFoundException {
		info = new BPlusTreeInfo(filename);
		root = new BPlusTreeLeafNodeP(info.rootPage, info);
	}

	public int size() {
		return size;
	}

	public Row get(Entry key) throws IOException {
		if (key == null) throw new IllegalArgumentException("argument key to get() is null");
		return root.get(key);
	}

	public void update(Entry key, Row value) throws IOException {
		root.remove(key);
		root.put(key, value);
	}

	public void put(Entry key, Row value) throws IOException {
		if (key == null) throw new IllegalArgumentException("argument key to put() is null");
		root.put(key, value);
		size++;
		checkRoot();
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


	public void remove(Entry key) throws IOException {
		if (key == null) throw new IllegalArgumentException("argument key to remove() is null");
		root.remove(key);
		size--;
		if (root instanceof BPlusTreeInternalNodeP && root.size() == 0) {
			info.rootPage = ((BPlusTreeInternalNodeP) root).children.get(0);
			root = page2instance(info.rootPage);
			info.write();
		}
	}

	public boolean contains(Entry key) throws IOException {
		if (key == null) throw new IllegalArgumentException("argument key to contains() is null");
		return root.containsKey(key);
	}

	private void checkRoot() throws IOException {
		if (root.isOverFlow()) {
			Pair<Integer, Entry> newSiblingNode = root.split();
			BPlusTreeInternalNodeP newRoot = new BPlusTreeInternalNodeP(info, 1);
			newRoot.keys.set(0, newSiblingNode.getValue());
			newRoot.children.set(0, info.rootPage);
			newRoot.children.set(1, newSiblingNode.getKey());
			newRoot.write();
			root.write();
			root = newRoot;
			info.rootPage = newRoot.pageId;
			info.write();
		}
	}

//	@Override
//	public BPlusTreeIteratorP iterator() {
//		return new BPlusTreeIteratorP(this);
//	}

	public BPlusTreeIteratorP find(Entry key) throws IOException {
		if(size == 1){
			return new BPlusTreeIteratorP((BPlusTreeLeafNodeP) root);
		}
		return new BPlusTreeIteratorP(((BPlusTreeInternalNodeP)root).find(key));
	}
}
