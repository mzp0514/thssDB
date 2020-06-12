package cn.edu.thssdb.index;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.type.BPlusNodeType;
import javafx.util.Pair;

import java.io.IOException;

public final class BPlusTreeP implements Iterable<Pair<Entry, Row>> {
//public final class BPlusTreeP {
	BPlusTreeNodeP root;
	BPlusTreeInfo info;
	//int size = 0;
	// node type(4)|prev page(4)|next page(4)|node size(4)

	public BPlusTreeP(String filename, int keyId, Column[] columns) throws IOException {
		info = new BPlusTreeInfo(filename, keyId, columns, columns[keyId].getMaxLength());
		root = new BPlusTreeLeafNodeP(info, 0);
		root.write();
	}

	public BPlusTreeP(String filename) throws IOException, ClassNotFoundException {
		info = new BPlusTreeInfo(filename);
		root = page2instance(info.rootPage);

//		int type = info.readNodeType(info.rootPage);
////
////		if(type == BPlusNodeType.LEAF.ordinal()){
////			root = new BPlusTreeLeafNodeP(info.rootPage, info);
////		}
////		else{
////			root = new BPlusTreeInternalNodeP(info.rootPage, info);
////		}
	}

	public void close() throws IOException {
		persist();
		this.info.close();
	}

	public void persist() throws IOException {
		this.info.write();
		this.info.writeCache();
		if(!this.info.cache.containsKey(this.root.pageId)){
			this.root.write();
		}
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
		//size++;
		checkRoot();
	}

	BPlusTreeNodeP page2instance(int page) throws IOException {
		BPlusTreeNodeP child = info.cache.get(page);
		if(child == null) {
			int type = info.readNodeType(page);
			if (type == BPlusNodeType.LEAF.ordinal()) {
				child = new BPlusTreeLeafNodeP(page, info);
			} else {
				child = new BPlusTreeInternalNodeP(page, info);
			}
			//info.cache.put(page, child);
		}
		return child;
	}


	public void remove(Entry key) throws IOException {
		if (key == null) throw new IllegalArgumentException("argument key to remove() is null");
		root.remove(key);
		//size--;
		if (root instanceof BPlusTreeInternalNodeP && root.size() == 0) {
			info.rootPage = ((BPlusTreeInternalNodeP) root).children.get(0);
			root = page2instance(info.rootPage);
			//info.write();
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
			//newRoot.write();
			//root.write();
			root = newRoot;
			//info.cache.put(newRoot.pageId, newRoot);
			info.rootPage = newRoot.pageId;
			// info.write();
		}
	}

	@Override
	public BPlusTreeIteratorP iterator() {
		try {
			return new BPlusTreeIteratorP(this);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public BPlusTreeIteratorP find(Entry key) throws IOException {
		if(root instanceof BPlusTreeLeafNodeP){
			return new BPlusTreeIteratorP((BPlusTreeLeafNodeP) root);
		}
		return new BPlusTreeIteratorP(((BPlusTreeInternalNodeP)root).find(key));
	}

//	public int size() {
//		return size;
//	}
}
