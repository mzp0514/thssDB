package cn.edu.thssdb.index;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.type.BPlusNodeType;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.Global;
import javafx.util.Pair;

import javax.xml.bind.annotation.XmlElementDecl;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

public class BPlusTreeInfo {

	int pageSize;
	int leafDataMaxLen;
	int internalDataMaxLen;
	int rootPage;

	LinkedList<Integer> freePages;
	HashMap<Integer, BPlusTreeNodeP> cache;

	int pageNum;
	ColumnType keyType;
	int keyId;
	File headerFile;
	RandomAccessFile indexFile;

	Column[] columns;

	BPlusTreeInfo(String filename) throws IOException, ClassNotFoundException {
		headerFile = new File(filename + ".header");
		indexFile = new RandomAccessFile(filename +".tree", "rw");
		cache = new HashMap<>();
		read();
		this.keyType = columns[keyId].getType();
	}

	BPlusTreeInfo(String filename, int keyId, Column[] columns, int stringMaxLen)
					throws IOException {

		this.columns = columns;
		this.keyId = keyId;
		this.keyType = columns[keyId].getType();
		cache = new HashMap<>();

		headerFile = new File(filename + ".header");
		indexFile = new RandomAccessFile(filename +".tree", "rw");

		freePages = new LinkedList<>();

		// internal
		int internalDataSize = Global.INT_SIZE;
		int leafDataSize = 0;
		switch (keyType){
			case INT:
				internalDataSize += Global.INT_SIZE;
				leafDataSize = Global.INT_SIZE;
				break;
			case FLOAT:
				internalDataSize += Global.FLOAT_SIZE;
				leafDataSize = Global.FLOAT_SIZE;
				break;
			case DOUBLE:
				internalDataSize += Global.DOUBLE_SIZE;
				leafDataSize = Global.DOUBLE_SIZE;
				break;
			case LONG:
				internalDataSize += Global.LONG_SIZE;
				leafDataSize = Global.LONG_SIZE;
				break;
			case STRING:
				internalDataSize += stringMaxLen;
				leafDataSize = stringMaxLen;
				break;
		}

		// leaf data

		for(Column col: columns){
			switch (col.getType()){
				case STRING:
					leafDataSize += col.getMaxLength() + Global.BYTE_SIZE;
					break;

				case INT:
					leafDataSize += Global.INT_SIZE + Global.BYTE_SIZE;
					break;

				case LONG:
					leafDataSize += Global.LONG_SIZE + Global.BYTE_SIZE;
					break;

				case FLOAT:
					leafDataSize += Global.FLOAT_SIZE + Global.BYTE_SIZE;
					break;

				case DOUBLE:
					leafDataSize += Global.DOUBLE_SIZE + Global.BYTE_SIZE;
					break;
			}
		}

		pageSize = Global.PAGE_SIZE;
		if((pageSize - Global.PAGE_HEADER_SIZE) / leafDataSize <= 3){
			pageSize *= 2;
		}

		internalDataMaxLen = (pageSize - Global.PAGE_HEADER_SIZE - Global.INT_SIZE) / internalDataSize;
		leafDataMaxLen = (pageSize -Global.PAGE_HEADER_SIZE) / leafDataSize;

		write();
	}

	void writeCache() throws IOException {
		Iterator iter = cache.entrySet().iterator();
		while (iter.hasNext()) {
			HashMap.Entry entry = (HashMap.Entry) iter.next();
			BPlusTreeNodeP val = (BPlusTreeNodeP) entry.getValue();
			val.write();
		}
		cache.clear();
	}

	int findFreePage() throws IOException {
		int pageID = 0;
		if(freePages.size() != 0){
			pageID = freePages.getFirst();
			freePages.removeFirst();
		}
		else{
			pageID = pageNum;
			pageNum ++;
		}

		write();
		return pageID;
	}

	void writeIndex(Object a, ColumnType type, long pos, int stringMaxLen) throws IOException {
		if(pos != -1)
			indexFile.seek(pos);

		if(a == null){

			int len = 0;
			switch (type){
				case INT:
					len = Global.INT_SIZE;
					break;
				case STRING:
					len = stringMaxLen;
					break;
				case LONG:
					len = Global.LONG_SIZE;
					break;
				case DOUBLE:
					len = Global.DOUBLE_SIZE;
					break;
				case FLOAT:
					len = Global.FLOAT_SIZE;
					break;
			}
			byte[] bytes = new byte[len];
			indexFile.write(bytes);
			return;
		}

		switch (type){
			case INT:
				indexFile.writeInt((Integer) a);
				break;
			case STRING:
				byte[] bytes = new byte[stringMaxLen];
				byte[] bytes_t = ((String) a).getBytes();
				for(int i = 0; i < bytes_t.length; i++){
					bytes[i] = bytes_t[i];
				}
				indexFile.write(bytes);
				break;
			case LONG:
				indexFile.writeLong((Long) a);
				break;
			case DOUBLE:
				indexFile.writeDouble((Double) a);
				break;
			case FLOAT:
				indexFile.writeFloat((Float) a);
				break;
			case BYTE:
				indexFile.writeByte((Byte) a);
				break;
		}
	}

	Object readIndex(ColumnType type, long pos, int stringMaxLen) throws IOException {
		//indexFile.seek(0);

		if(pos != -1)
			indexFile.seek(pos);

		Object a = null;

		switch (type){
			case INT:
				a = indexFile.readInt();
				break;

			case STRING:
				byte[] b = new byte[stringMaxLen];
				indexFile.read(b);
				a = new String(b);
				a = ((String) a).trim();
				break;

			case LONG:
				a = indexFile.readLong();
				break;

			case DOUBLE:
				a = indexFile.readDouble();
				break;

			case FLOAT:
				a = indexFile.readFloat();
				break;

			case BYTE:
				a = indexFile.readByte();
				break;
		}
		return a;
	}

	int readNodeType(int pageId) throws IOException {
		return (int) readIndex(ColumnType.INT, pageId * pageSize, -1);
	}

	public void write() throws IOException {
		FileOutputStream fs2 = new FileOutputStream(headerFile);
		ObjectOutputStream os2 =  new ObjectOutputStream(fs2);
		os2.writeInt(pageSize);
		os2.writeInt(leafDataMaxLen);
		os2.writeInt(internalDataMaxLen);
		os2.writeInt(pageNum);
		os2.writeInt(keyId);
		os2.writeInt(rootPage);
		os2.writeObject(freePages);
		os2.writeObject(columns);
		os2.close();
		fs2.close();
	}

	public void read() throws IOException, ClassNotFoundException {
		ObjectInputStream ois = new ObjectInputStream(new FileInputStream(headerFile));
		pageSize = ois.readInt();
		leafDataMaxLen = ois.readInt();
		internalDataMaxLen = ois.readInt();
		pageNum = ois.readInt();
		keyId = ois.readInt();
		rootPage = ois.readInt();
		freePages = (LinkedList<Integer>) ois.readObject();
		columns = (Column[]) ois.readObject();
		ois.close();
	}


	public void close() throws IOException {
		this.indexFile.close();
	}
}
