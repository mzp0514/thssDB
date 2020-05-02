package cn.edu.thssdb.schema;

import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;

import cn.edu.thssdb.schema.TableP;

import cn.edu.thssdb.exception.KeyNotExistException;

import java.nio.file.NotDirectoryException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import java.io.*;

public class Database {

  private String databaseName;
  private String filePath;
  private HashMap<String, TableP> tables;
  private HashSet<String> tableNames;
  private File dbDir;
  private File dbMeta;
  ReentrantReadWriteLock lock;

  public Database(String name) throws IOException, ClassNotFoundException {
    this.databaseName = name;
    this.filePath = new String("data/" + this.databaseName + '/');
    this.tables = new HashMap<>();
    this.tableNames = new HashSet<>();
    this.dbDir = new File(this.filePath);
    this.dbMeta = new File(this.filePath + this.databaseName + ".dbmeta");
    this.lock = new ReentrantReadWriteLock();
    recover();
  }

  // 更新meta文件
  private void persist() throws IOException {
    // TODO
    this.lock.writeLock().lock();
    FileOutputStream fs1 = new FileOutputStream(this.filePath + this.databaseName + ".dbmeta");
    ObjectOutputStream os1 =  new ObjectOutputStream(fs1);
    os1.writeObject(this.tableNames);
    os1.close();
    this.lock.writeLock().unlock();
  }


  // 创建一个新的table，绑定至该数据库。
  public void create(String name, Column[] columns) throws IOException {
    // TODO
    TableP newTb = new TableP(this.databaseName, name, columns);
    this.tableNames.add(name);
    this.tables.put(name, newTb);
    persist();
  }


  public void drop(String name) {
    // TODO
    if (!this.tableNames.contains(name))
      throw new KeyNotExistException();
    if (this.tables.containsKey(name))
    {
        TableP tb = this.tables.remove(name);
        tb = null;
    }



  }

  public String select(QueryTable[] queryTables) {
    // TODO
    QueryResult queryResult = new QueryResult(queryTables);
    return null;
  }

  // 根据name来恢复数据库，若没有，则创建新数据库。
  private void recover() throws IOException, ClassNotFoundException {
    // TODO

    // 数据库存在
    if (this.dbDir.exists() && this.dbDir.isDirectory() && this.dbMeta.exists() && this.dbMeta.isFile())
    {
      this.lock.readLock().lock();
      ObjectInputStream ois = new ObjectInputStream(new FileInputStream(this.dbMeta));
      this.tableNames = (HashSet<String>) ois.readObject();
      ois.close();
      this.lock.readLock().unlock();
      for (String tbn : this.tableNames)
      {
        TableP tb = new TableP(this.databaseName, tbn);
        this.tables.put(tbn, tb);
      }
    }
    // 数据库不存在
    else if (!this.dbDir.exists() && !dbMeta.exists())
    {
      dbDir.mkdir();
      dbMeta.createNewFile();
    }
    // 文件结构异常
    else
      throw new NotDirectoryException("Error: invalid file structure");


  }

  public void quit() {
    // TODO
  }
}
