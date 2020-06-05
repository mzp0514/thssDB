package cn.edu.thssdb.index;

import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.type.BPlusNodeType;
import cn.edu.thssdb.type.ColumnType;
import javafx.util.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public final class BPlusTreeInternalNodeP extends BPlusTreeNodeP {

	ArrayList<Integer> children;

	BPlusTreeInternalNodeP(BPlusTreeInfo info, int size) throws IOException {
		this.pageId = info.findFreePage();
		this.info = info;
		this.nodeType = BPlusNodeType.INTERNAL.ordinal();
		keys = new ArrayList<>(Collections.nCopies((int) (1.5 * info.internalDataMaxLen), null));
		children = new ArrayList<>(Collections.nCopies((int) (1.5 * info.internalDataMaxLen + 1), -1));
//		write();
		nodeSize = size;
		info.cache.put(pageId, this);
	}

	BPlusTreeInternalNodeP(int pageId, BPlusTreeInfo info) throws IOException {
		this.pageId = pageId;
		this.info = info;
		this.nodeType = BPlusNodeType.INTERNAL.ordinal();
		keys = new ArrayList<>(Collections.nCopies((int) (1.5 * info.internalDataMaxLen), null));
		children = new ArrayList<>(Collections.nCopies((int) (1.5 * info.internalDataMaxLen + 1), -1));
		read();
		info.cache.put(pageId, this);
	}

	@Override
	void write() throws IOException {

		info.writeIndex(BPlusNodeType.INTERNAL.ordinal(), ColumnType.INT, pageId * info.pageSize, -1);
		info.writeIndex(prevPage, ColumnType.INT, -1, -1);
		info.writeIndex(nextPage, ColumnType.INT, -1, -1);
		info.writeIndex(nodeSize, ColumnType.INT, -1, -1);

		for(int i = 0; i < nodeSize; i++){
			info.writeIndex(keys.get(i).value, info.keyType, -1, info.columns[info.keyId].getMaxLength());
		}

		for(int i = 0; i < nodeSize + 1; i++){
			info.writeIndex(children.get(i), ColumnType.INT, -1, info.columns[info.keyId].getMaxLength());
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
		}

		for(int i = 0; i < nodeSize + 1; i++){
			Object k = info.readIndex(ColumnType.INT, -1, -1);
			children.set(i, (int) k);
		}
	}

	private void childrenAdd(int index, int node) {
		for (int i = nodeSize + 1; i > index; i--) {
			children.set(i, children.get(i - 1));
		}
		children.set(index, node);
	}

	private void childrenRemove(int index) {
		for (int i = index; i < nodeSize; i++) {
			children.set(i, children.get(i + 1));
		}
	}

	@Override
	boolean containsKey(Entry key) throws IOException {
		BPlusTreeNodeP child = getChildInstance(key);
		return child.containsKey(key);
	}

	@Override
	Row get(Entry key) throws IOException {
		BPlusTreeNodeP child = getChildInstance(key);

		return child.get(key);
	}



	private BPlusTreeNodeP getChildInstance(Entry key) throws IOException {
		int childPage = searchChild(key);
		return page2instance(childPage);
	}

	BPlusTreeLeafNodeP find(Entry key) throws IOException {
		BPlusTreeNodeP node = getChildInstance(key);
		if(node instanceof BPlusTreeLeafNodeP){
			return (BPlusTreeLeafNodeP) node;
		}
		else{
			return ((BPlusTreeInternalNodeP)node).find(key);
		}
	}

	@Override
	void put(Entry key, Row value) throws IOException {
		BPlusTreeNodeP child = getChildInstance(key);
		child.put(key, value);
		if (child.isOverFlow()) {
			Pair<Integer, Entry> newSiblingNode = child.split();
			insertChild(newSiblingNode.getValue(), newSiblingNode.getKey());
		}
//		child.write();
//		write();
	}

	@Override
	void remove(Entry key) throws IOException {
		int index = binarySearch(key);
		int childIndex = index >= 0 ? index + 1 : -index - 1;
		int childPage = children.get(childIndex);

//		int type = info.readNodeType(childPage);
		BPlusTreeNodeP child = page2instance(childPage);
//		if(type == BPlusNodeType.LEAF.ordinal()){
//			child = new BPlusTreeLeafNodeP(childPage, info);
//		}
//		else{
//			child = new BPlusTreeInternalNodeP(childPage, info);
//		}

		child.remove(key);
		if (child.isUnderFlow()) {
			BPlusTreeNodeP childLeftSibling = getChildLeftSibling(key);
			BPlusTreeNodeP childRightSibling = getChildRightSibling(key);
			BPlusTreeNodeP left = childLeftSibling != null ? childLeftSibling : child;
			BPlusTreeNodeP right = childLeftSibling != null ? child : childRightSibling;

			left.merge(right);
			if (index >= 0) {
				childrenRemove(index + 1);
				keysRemove(index);
			} else {
				assert right != null;
				deleteChild(right.getFirstLeafKey());
			}
			if (left.isOverFlow()) {
				Pair<Integer, Entry> newSiblingNode = left.split();
				insertChild(newSiblingNode.getValue(), newSiblingNode.getKey());
			}
//			left.write();
//			right.write();
		}
		else if (index >= 0) {
			keys.set(index, page2instance(children.get(index + 1)).getFirstLeafKey());
		}
//		write();
//		child.write();
	}

	@Override
	Entry getFirstLeafKey() throws IOException {
		return page2instance(children.get(0)).getFirstLeafKey();
	}



	@Override
	Pair<Integer, Entry> split() throws IOException {
		int from = size() / 2 + 1;
		int to = size();
		BPlusTreeInternalNodeP newSiblingNode = new BPlusTreeInternalNodeP(info, to - from);

		for (int i = 0; i < to - from; i++) {
			newSiblingNode.keys.set(i, keys.get(i + from));
			newSiblingNode.children.set(i, children.get(i + from));
		}
		newSiblingNode.children.set(to - from, children.get(to));
		this.nodeSize = this.nodeSize - to + from - 1;
		return new Pair<>(newSiblingNode.pageId, newSiblingNode.getFirstLeafKey());
	}

	@Override
	void merge(BPlusTreeNodeP sibling) throws IOException {
		int index = nodeSize;
		BPlusTreeInternalNodeP node = (BPlusTreeInternalNodeP) sibling;
		int length = node.nodeSize;
		keys.set(index, node.getFirstLeafKey());
		for (int i = 0; i < length; i++) {
			keys.set(i + index + 1, node.keys.get(i));
			children.set(i + index + 1, node.children.get(i));
		}

		children.set(length + index + 1, node.children.get(length));
		nodeSize = index + length + 1;
		info.freePages.add(node.pageId);
		info.cache.remove(node.pageId);
//		info.write();
	}

	@Override
	boolean isOverFlow() {
		return nodeSize > info.internalDataMaxLen;
	}

	@Override
	boolean isUnderFlow() {
		return nodeSize < info.internalDataMaxLen / 2;
	}

	BPlusTreeLeafNodeP getFirstLeaf() throws IOException {
		BPlusTreeNodeP node = page2instance(children.get(0));
		if(node instanceof BPlusTreeLeafNodeP){
			return (BPlusTreeLeafNodeP) node;
		}
		else{
			return ((BPlusTreeInternalNodeP) node).getFirstLeaf();
		}
	}

	private int searchChild(Entry key) {
		int index = binarySearch(key);
		return children.get(index >= 0 ? index + 1 : -index - 1);
	}

	private void insertChild(Entry key, int child) throws IOException {
		int index = binarySearch(key);
		int childIndex = index >= 0 ? index + 1 : -index - 1;
		if (index >= 0) {
			children.set(childIndex, child);
		} else {
			childrenAdd(childIndex + 1, child);
			keysAdd(childIndex, key);
		}
//		write();
	}

	private void deleteChild(Entry key) throws IOException {
		int index = binarySearch(key);
		if (index >= 0) {
			childrenRemove(index + 1);
			keysRemove(index);
//			write();
		}
	}

	private BPlusTreeNodeP getChildLeftSibling(Entry key) throws IOException {
		int index = binarySearch(key);
		int childIndex = index >= 0 ? index + 1 : -index - 1;
		if (childIndex > 0)
			return page2instance(children.get(childIndex - 1));
		return null;
	}

	private BPlusTreeNodeP getChildRightSibling(Entry key) throws IOException {
		int index = binarySearch(key);
		int childIndex = index >= 0 ? index + 1 : -index - 1;
		if (childIndex < size())
			return page2instance(children.get(childIndex + 1));
		return null;
	}
}
