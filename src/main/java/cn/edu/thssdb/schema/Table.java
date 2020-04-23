package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DuplicateColumnNameException;
import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.index.BPlusTreeIterator;
import cn.edu.thssdb.type.ComparisonType;
import com.sun.corba.se.spi.ior.ObjectKey;
import javafx.util.Pair;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class Table implements Iterable<Row> {
	ReentrantReadWriteLock lock;
	private String databaseName;
	public String tableName;
	public String filePath;
	public ArrayList<Column> columns;
	public BPlusTree<Entry, Row> index;
	public int primaryKey;
	File metaFile;
	File dataFile;


	public Table(String databaseName, String tableName, Column[] columns) throws IOException {
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
		File dataFile = new File(this.filePath + this.tableName + ".data");
		if(!metaFile.exists())
			metaFile.createNewFile();
		if(!dataFile.exists())
			dataFile.createNewFile();

		metaSerialize();
		this.index = new BPlusTree<>();
	}

	Table(String databaseName, String tableName) throws IOException, ClassNotFoundException {
		// TODO
		this.databaseName = new String(databaseName);
		this.tableName = new String(tableName);

		this.filePath = new String("data/" + this.databaseName + "/");
		this.index = new BPlusTree<>();

		this.metaFile = new File(this.filePath + this.tableName + ".meta");
		this.dataFile = new File(this.filePath + this.tableName + ".data");

		if(metaFile.exists() && dataFile.exists()){
			this.metaDeserialize();

			ArrayList<Pair<Entry, Row>> res = this.deserialize();
			for(Pair<Entry, Row> u: res){
				this.index.put(u.getKey(), u.getValue());
			}
		}

	}

	public void insert(Row row) throws IOException {
		// TODO
		ArrayList<Entry> entries = row.getEntries();
		this.index.put(entries.get(this.primaryKey), row);
		this.serialize();
	}

	public void delete(Row row) throws IOException{
		try {
			this.index.remove(row.getEntries().get(this.primaryKey));
		}
		catch (Exception e){

		}
		this.serialize();
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
		this.serialize();
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
		this.serialize();
	}

	public ArrayList<Row> select(String attrName, Object attrValue, ComparisonType comp) {
		ArrayList<Row> res = new ArrayList<>();

		int attrId = this.getAttrIndex(attrName);

		Entry entry = new Entry((Comparable) attrValue);

		BPlusTreeIterator<Entry, Row> it = this.index.iterator();

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

					while(it.hasNext()){
						Row temp = it.next().getValue();
						if(temp.getEntries().get(attrId).equals(entry)){
							res.add(temp);
						}
					}
				}
				break;

			case NEQUAL:

				while(it.hasNext()){
					Row temp = it.next().getValue();
					if(!temp.getEntries().get(attrId).equals(entry)){
						res.add(temp);
					}
				}
				break;

			case LESS:

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
					BPlusTreeIterator<Entry, Row> it2 = this.index.find(entry);
					while(it2.hasNext()){
						Row temp = it2.next().getValue();
						if(temp.getEntries().get(attrId).compareTo(entry) != -1 ){
							res.add(temp);
							while(it2.hasNext()){
								res.add(it2.next().getValue());
							}
						}
					}
				}
				else{
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
					BPlusTreeIterator<Entry, Row> it2 = this.index.find(entry);
					while(it2.hasNext()){
						Row temp = it2.next().getValue();
						if(temp.getEntries().get(attrId).compareTo(entry) == 1 ){
							res.add(temp);
							while(it2.hasNext()){
								res.add(it2.next().getValue());
							}
						}
					}
				}
				else{
					while(it.hasNext()){
						Row temp = it.next().getValue();
						if(temp.getEntries().get(attrId).compareTo(entry) == 1 ){
							res.add(temp);
						}
					}
				}
				break;

			case NGREATER:
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

	private void serialize() throws IOException {
		// TODO
		FileOutputStream fs2 = new FileOutputStream(this.filePath + this.tableName + ".data");
		ObjectOutputStream os2 =  new ObjectOutputStream(fs2);
		BPlusTreeIterator<Entry, Row> it = this.index.iterator();
		ArrayList<Pair<Entry, Row>> res = new ArrayList<>();
		while(it.hasNext()){
			res.add(it.next());
		}
		os2.writeObject(res);
		os2.close();
	}

	private void metaDeserialize() throws IOException, ClassNotFoundException {
		ObjectInputStream ois = new ObjectInputStream(new FileInputStream(this.metaFile));
		this.columns = (ArrayList<Column>) ois.readObject();
	}

	private ArrayList<Pair<Entry, Row>> deserialize() throws IOException, ClassNotFoundException {
		// TODO
		ObjectInputStream ois = new ObjectInputStream(new FileInputStream(this.dataFile));
		return (ArrayList<Pair<Entry, Row>>) ois.readObject();
	}

	private int getAttrIndex(String attrName){
		int attrId = 0;
		while(this.columns.get(attrId).getName() != attrName){
			attrId ++;
		}
		return attrId;
	}

	private class TableIterator implements Iterator<Row> {
		private Iterator<Pair<Entry, Row>> iterator;

		TableIterator(Table table) {
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
