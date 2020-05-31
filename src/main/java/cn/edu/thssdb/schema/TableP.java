package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DuplicateColumnNameException;

import cn.edu.thssdb.index.BPlusTreeIteratorP;
import cn.edu.thssdb.index.BPlusTreeP;
import cn.edu.thssdb.type.ComparisonType;
import cn.edu.thssdb.utils.Global;
import com.sun.corba.se.spi.ior.ObjectKey;
import javafx.util.Pair;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Stack;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class TableP implements Iterable<Row> {
	ReentrantReadWriteLock lock;
	private String databaseName;
	public String tableName;
	public String filePath;
	public ArrayList<Column> columns;
	public BPlusTreeP index;
	public int primaryKey;
	public long currentSessionID;
	public Stack<ArrayList<Row>> rowsForActions;
	public Stack<ArrayList<Row>> rowsForActionsAppend;
	public Stack<Global.STATE_TYPE> actionType;
	File metaFile;

	public TableP(String databaseName, String tableName, Column[] columns) throws IOException {
		// TODO
		this.databaseName = new String(databaseName);
		this.tableName = new String(tableName);
		this.currentSessionID = -1;
		this.rowsForActions = new Stack<>();
		this.rowsForActionsAppend = new Stack<>();
		this.actionType = new Stack<>();

		this.filePath = new String("data/" + this.databaseName + "/");

		this.columns = new ArrayList<>();
		HashSet<String> nameSet = new HashSet<>();
		for (int i = 0; i < columns.length; i++) {
			this.columns.add(columns[i]);
			nameSet.add(columns[i].getName());
			if(columns[i].isPrimary()){
				this.primaryKey = i;
			}
		}
		if (nameSet.size() != this.columns.size()) {
			throw new DuplicateColumnNameException();
		}

		File metaFile = new File(this.filePath + this.tableName + ".meta");

		if(!metaFile.exists())
			metaFile.createNewFile();

		metaSerialize();

		index = new BPlusTreeP(this.filePath + this.tableName + "index", primaryKey, columns);
	}

	public TableP(String databaseName, String tableName) throws IOException, ClassNotFoundException {

		this.databaseName = new String(databaseName);
		this.tableName = new String(tableName);
		this.currentSessionID = -1;
		this.rowsForActions = new Stack<>();
		this.rowsForActionsAppend = new Stack<>();
		this.actionType = new Stack<>();

		this.filePath = new String("data/" + this.databaseName + "/");

		this.metaFile = new File(this.filePath + this.tableName + ".meta");

		if (metaFile.exists()) {
			this.metaDeserialize();
		}

		index = new BPlusTreeP(this.filePath + this.tableName + "index");

	}

	public void close() throws IOException {
		this.index.close();
	}

	public void insert(Row row) throws IOException {
		// TODO
		ArrayList<Entry> entries = row.getEntries();

		this.index.put(entries.get(this.primaryKey), row);

	}

	public void insert(ArrayList<Row> rows) throws IOException {
		// TODO
		for (Row row: rows) {
			ArrayList<Entry> entries = row.getEntries();

			this.index.put(entries.get(this.primaryKey), row);
		}

	}

	public void delete(Row row) throws IOException{
		try {
			this.index.remove(row.getEntries().get(this.primaryKey));
		}
		catch (Exception e){

		}
	}

	public void delete(ArrayList<Row> rows) throws IOException {
		// TODO
		for(Row row: rows) {
			try {
				this.index.remove(row.getEntries().get(this.primaryKey));
			}
			catch (Exception e){
				continue;
			}
		}
	}


	public void update(ArrayList<Row> rows, String attrName, Object attrValue) throws IOException {
		// TODO
		ArrayList<Row> res = new ArrayList<>();
		for(Row row : rows) {
			int attrId = this.getAttrIndex(attrName);
			Entry key = row.getEntries().get(this.primaryKey);
			Row dest = row;
			dest.updateEntry(attrId, new Entry((Comparable) attrValue));
			res.add(dest);
			this.index.update(key, dest);
		}
		this.rowsForActionsAppend.push(res);
	}


	public ArrayList<Row> match(ArrayList<Integer> attrs, ArrayList<Object> vals) throws IOException {
		ArrayList<Row> res = new ArrayList<>();
		BPlusTreeIteratorP it = index.iterator();
		int pri_idx = attrs.indexOf(primaryKey);
		if(pri_idx != -1){
			try {
				Row r = this.index.get((Entry) vals.get(pri_idx));
				if (attrs.size() > 1) {
					Boolean matched = true;
					for (int i = 0; i < attrs.size(); i++) {
						matched = (r.getEntries().get(attrs.get(i)).equals((Comparable) vals.get(i)));
						if (!matched) {
							break;
						}
					}
					if (matched) {
						res.add(r);
					}
				} else {
					res.add(r);
				}
			}
			catch (Exception e){

			}
		}
		else {
			while (it.hasNext()) {
				Row temp = it.next().getValue();
				Boolean matched = true;
				for (int i = 0; i < attrs.size(); i++) {

					matched = (temp.getEntries().get(attrs.get(i)).equals((Comparable) vals.get(i)));

					if (!matched) {
						break;
					}
				}
				if (matched) {
					res.add(temp);
				}
			}
		}
		return res;
	}

	public ArrayList<Row> select(int attrId, Object attrValue, ComparisonType comp) throws IOException {
		ArrayList<Row> res = new ArrayList<>();

		Entry entry = new Entry((Comparable) attrValue);

		BPlusTreeIteratorP it;

		switch (comp){
			case EQUAL:
				if(attrId == this.primaryKey){
					try{
						res.add(this.index.get(entry));
					}
					catch (Exception e){

					}
				}
				else{
					it = index.iterator();
					while(it.hasNext()){

						Row temp = it.next().getValue();
						if(temp.getEntries().get(attrId).equals(entry)){
							res.add(temp);
						}
					}
				}
				break;

			case NEQUAL:
				it = index.iterator();
				while(it.hasNext()){
					Row temp = it.next().getValue();
					if(!temp.getEntries().get(attrId).equals(entry)){
						res.add(temp);
					}
				}
				break;

			case LESS:
				it = index.iterator();
				while(it.hasNext()){
					Row temp = it.next().getValue();
					if(temp.getEntries().get(attrId).compareTo(entry) == -1){
						res.add(temp);
					}
					else if (attrId == this.primaryKey){
						break;
					}
				}
				break;

			case NLESS:
				if(attrId == this.primaryKey){
					it = this.index.find(entry);
					while(it.hasNext()){
						Row temp = it.next().getValue();
						if(temp.getEntries().get(attrId).compareTo(entry) != -1 ){
							res.add(temp);
							while(it.hasNext()){
								res.add(it.next().getValue());
							}
						}
					}
				}
				else{
					it = index.iterator();
					while(it.hasNext()){
						Row temp = it.next().getValue();
						if(temp.getEntries().get(attrId).compareTo(entry) != -1 ){
							res.add(temp);
						}
					}
				}

				break;

			case GREATER:

				if(attrId == this.primaryKey){
					it = this.index.find(entry);
					while(it.hasNext()){
						Row temp = it.next().getValue();
						if(temp.getEntries().get(attrId).compareTo(entry) == 1 ){
							res.add(temp);
							while(it.hasNext()){
								res.add(it.next().getValue());
							}
						}
					}
				}
				else{
					it = index.iterator();
					while(it.hasNext()){
						Row temp = it.next().getValue();
						if(temp.getEntries().get(attrId).compareTo(entry) == 1 ){
							res.add(temp);
						}
					}
				}
				break;

			case NGREATER:
				it = index.iterator();
				while(it.hasNext()){
					Row temp = it.next().getValue();
					if(temp.getEntries().get(attrId).compareTo(entry) != 1){
						res.add(temp);
					}
					else if (attrId == this.primaryKey){
						break;
					}
				}
				break;
		}

		return res;
	}

	private void metaSerialize() throws IOException {
		FileOutputStream fs1 = new FileOutputStream(this.filePath + this.tableName + ".meta");
		ObjectOutputStream os1 =  new ObjectOutputStream(fs1);
		os1.writeObject(this.columns);
		os1.writeInt(this.primaryKey);
		os1.close();
		fs1.close();
	}

	private void metaDeserialize() throws IOException, ClassNotFoundException {
		ObjectInputStream ois = new ObjectInputStream(new FileInputStream(this.metaFile));
		this.columns = (ArrayList<Column>) ois.readObject();
		this.primaryKey = ois.readInt();
		ois.close();
	}

	public int getAttrIndex(String attrName){
		int attrId = 0;

		while(attrId < columns.size() && !columns.get(attrId).getName().equals(attrName)){
			attrId ++;
		}
		if(attrId < columns.size()) {
			return attrId;
		}
		else {
			return -1;
		}
	}

	private class TableIterator implements Iterator<Row> {
		private Iterator<Pair<Entry, Row>> iterator;

		TableIterator(TableP table) {
			this.iterator = table.index.iterator();
		}

		@Override
		public boolean hasNext() {
			return iterator.hasNext();
		}

		@Override
		public Row next() {
			return iterator.next().getValue();
		}
	}

	@Override
	public Iterator<Row> iterator() {
		return new TableIterator(this);
	}


	public ArrayList<Column> getColumns() { return columns; }


	public void persist() throws IOException {
		this.actionType.clear();
		this.rowsForActions.clear();
		this.rowsForActionsAppend.clear();
		this.index.persist();
		this.currentSessionID = -1;
	}
}
