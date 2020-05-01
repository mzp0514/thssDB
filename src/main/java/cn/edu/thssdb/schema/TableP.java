package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DuplicateColumnNameException;

import cn.edu.thssdb.index.BPlusTreeIteratorP;
import cn.edu.thssdb.index.BPlusTreeP;
import cn.edu.thssdb.type.ComparisonType;
import com.sun.corba.se.spi.ior.ObjectKey;
import javafx.util.Pair;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class TableP implements Iterable<Row> {
	ReentrantReadWriteLock lock;
	private String databaseName;
	public String tableName;
	public String filePath;
	public ArrayList<Column> columns;
	public BPlusTreeP index;
	public int primaryKey;
	File metaFile;

	public TableP(String databaseName, String tableName, Column[] columns) throws IOException {
		// TODO
		this.databaseName = new String(databaseName);
		this.tableName = new String(tableName);

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

		index = new BPlusTreeP(this.filePath + "index", primaryKey, columns);
	}

	TableP(String databaseName, String tableName) throws IOException, ClassNotFoundException {

		this.databaseName = new String(databaseName);
		this.tableName = new String(tableName);

		this.filePath = new String("data/" + this.databaseName + "/");

		this.metaFile = new File(this.filePath + this.tableName + ".meta");

		if(metaFile.exists()){
			this.metaDeserialize();
		}

		index = new BPlusTreeP(this.filePath + "index");

	}

	public void insert(Row row) throws IOException {
		// TODO
		ArrayList<Entry> entries = row.getEntries();
		this.index.put(entries.get(this.primaryKey), row);
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
		for(Row row : rows) {
			int attrId = this.getAttrIndex(attrName);
			Entry key = row.getEntries().get(this.primaryKey);
			Row dest = row;
			dest.updateEntry(attrId, new Entry((Comparable) attrValue));
			this.index.update(key, dest);
		}
	}

	public ArrayList<Row> select(String attrName, Object attrValue, ComparisonType comp) throws IOException {
		ArrayList<Row> res = new ArrayList<>();

		int attrId = this.getAttrIndex(attrName);

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
		os1.close();
	}

	private void metaDeserialize() throws IOException, ClassNotFoundException {
		ObjectInputStream ois = new ObjectInputStream(new FileInputStream(this.metaFile));
		this.columns = (ArrayList<Column>) ois.readObject();
	}

	private int getAttrIndex(String attrName){
		int attrId = 0;
		while(!this.columns.get(attrId).getName().equals(attrName)){
			attrId ++;
		}
		return attrId;
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
}
