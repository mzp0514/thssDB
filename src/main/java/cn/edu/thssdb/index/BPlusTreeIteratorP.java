package cn.edu.thssdb.index;

import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import javafx.util.Pair;

import java.util.Iterator;
import java.util.LinkedList;

public class BPlusTreeIteratorP {
	private LinkedList<BPlusTreeLeafNodeP> queue;
	private LinkedList<Pair<Entry, Row>> buffer;

	BPlusTreeIteratorP(BPlusTreeP tree) {
//		queue = new LinkedList<>();
//		buffer = new LinkedList<>();
//		if (tree.size() == 0)
//			return;
//		BPlusTreeNodeP node = tree.root;
//		while(node instanceof BPlusTreeInternalNodeP){
//			node = ((BPlusTreeInternalNodeP) node).children.get(0);
//		}
//		queue.add((BPlusTreeLeafNodeP) node);
	}

	BPlusTreeIteratorP(BPlusTreeLeafNodeP node){
//		queue = new LinkedList<>();
//		buffer = new LinkedList<>();
//
//		queue.add(node);
	}

//	@Override
//	public boolean hasNext() {
//		return !queue.isEmpty() || !buffer.isEmpty();
//	}
//
//	@Override
//	public Pair<K, V> next() {
////		if (buffer.isEmpty()) {
////			while (true) {
////				BPlusTreeNode<K, V> node = queue.poll();
////				if (node instanceof BPlusTreeLeafNode) {
////					for (int i = 0; i < node.size(); i++)
////						buffer.add(new Pair<>(node.keys.get(i), ((BPlusTreeLeafNode<K, V>) node).values.get(i)));
////					break;
////				} else if (node instanceof BPlusTreeInternalNode)
////					for (int i = 0; i <= node.size(); i++)
////						queue.add(((BPlusTreeInternalNode<K, V>) node).children.get(i));
////			}
////		}
////		return buffer.poll();
//
//			if (buffer.isEmpty()) {
//					BPlusTreeLeafNodeP<K, V> node = queue.poll();
//
//					for (int i = 0; i < node.size(); i++)
//						buffer.add(new Pair<>(node.keys.get(i), node.values.get(i)));
//
//					if(node.hasNext()) {
//						queue.add(node.next());
//					}
//				}
//				return buffer.poll();
//	}
}
