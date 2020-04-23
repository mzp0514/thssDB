package cn.edu.thssdb.index;

import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import javafx.util.Pair;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

public class BPlusTreeIteratorP implements Iterator<Pair<Entry, Row>> {
	private LinkedList<BPlusTreeLeafNodeP> queue;
	private LinkedList<Pair<Entry, Row>> buffer;

	BPlusTreeIteratorP(BPlusTreeP tree) throws IOException {
		queue = new LinkedList<>();
		buffer = new LinkedList<>();

		if(tree.root instanceof BPlusTreeLeafNodeP){
			queue.add((BPlusTreeLeafNodeP) tree.root);
		}
		else{
			queue.add(((BPlusTreeInternalNodeP) tree.root).getFirstLeaf());
		}
	}

	BPlusTreeIteratorP(BPlusTreeLeafNodeP node){
		queue = new LinkedList<>();
		buffer = new LinkedList<>();

		queue.add(node);
	}

	@Override
	public boolean hasNext() {
		return !queue.isEmpty() || !buffer.isEmpty();
	}

	@Override
	public Pair<Entry, Row> next(){

			if (buffer.isEmpty()) {
					BPlusTreeLeafNodeP node = queue.poll();

					for (int i = 0; i < node.size(); i++)
						buffer.add(new Pair<>(node.keys.get(i), node.values.get(i)));

					if(node.hasNext()) {
						try {
							queue.add((BPlusTreeLeafNodeP) node.next());
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
				return buffer.poll();
	}
}
