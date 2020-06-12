package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.*;
import cn.edu.thssdb.query.QueryResult;


import cn.edu.thssdb.utils.Global;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import java.io.*;

class DBMeta  implements Serializable {
  public HashSet<String> tableNames;
  public Timestamp lastModifyTime;

  public DBMeta(HashSet<String> names, Timestamp timestamp)
  {
    this.tableNames = names;
    this.lastModifyTime = timestamp;
  }
}

public class Database {

  private String databaseName;
  private String filePath;
  private HashMap<String, TableP> tables;
  private HashSet<String> tableNames;
  private File dbDir;
  private File dbMeta;
  private Timestamp lastModifyTimeStamp;
  private String currentStatement;
  public TransactionManager txManager;
  public WALManager walManager;
  ReentrantReadWriteLock lock;

  public Database(String name) throws IOException, ClassNotFoundException {
    this.databaseName = name;
    this.filePath = new String("data/" + this.databaseName + '/');
    this.tables = new HashMap<>();
    this.tableNames = new HashSet<>();
    this.dbDir = new File(this.filePath);
    this.dbMeta = new File(this.filePath + this.databaseName + ".dbmeta");
    this.lock = new ReentrantReadWriteLock();
    this.txManager = new TransactionManager(this);
    this.walManager = new WALManager(this, this.filePath);
    recover();
    persist();
  }

  // 更新meta文件
  private void persist() throws IOException {
    // TODO
    DBMeta meta = new DBMeta(this.tableNames, this.lastModifyTimeStamp);
    this.lock.writeLock().lock();
    FileOutputStream fs1 = new FileOutputStream(this.filePath + this.databaseName + ".dbmeta");
    ObjectOutputStream os1 =  new ObjectOutputStream(fs1);
    os1.writeObject(meta);
    os1.close();
    fs1.close();
    this.lock.writeLock().unlock();
  }


  public TableP getTable(String tableName) throws IOException, ClassNotFoundException {
    if (!tableInDB(tableName))
      return null;
    if (this.tables.containsKey(tableName))
      return this.tables.get(tableName);
    else
    {
      TableP tb = new TableP(this.databaseName, tableName);
      this.addTable(tableName, tb);
      return this.tables.get(tableName);
    }
  }

  // 创建一个新的table，绑定至该数据库。
  public void create(String name, Column[] columns) throws IOException {
    // TODO
    if (this.tableNames.contains(name))
      throw new DuplicateKeyException();
    TableP newTb = new TableP(this.databaseName, name, columns);
    this.tableNames.add(name);
    addTable(name, newTb);
    this.lastModifyTimeStamp = new Timestamp(new Date().getTime());
    persist();
  }


  public void drop(String tbname) throws IOException {
    // TODO
    if (!this.tableNames.contains(tbname))
      throw new KeyNotExistException();
    if (this.tables.containsKey(tbname))
    {
        TableP tb = this.tables.remove(tbname);
        tb.close();
        tb = null;
    }
    FilenameFilter filter = new FilenameFilter() {
      @Override
      public boolean accept(File pathname, String name) {
        return name.startsWith(tbname);
      }
    };
    File[] files = this.dbDir.listFiles(filter);
    boolean flag = true;
    for (File file : files)
    {
        flag &= file.delete();
    }
    if (!flag)
      throw new FileDeleteFailedException();
    this.tableNames.remove(tbname);
    this.lastModifyTimeStamp = new Timestamp(new Date().getTime());
    persist();
  }

//  public String select(QueryTable[] queryTables) {
//    // TODO
//    QueryResult queryResult = new QueryResult(queryTables);
//    return null;
//  }

  // 根据name来恢复数据库，若没有，则创建新数据库。
  private void recover() throws IOException, ClassNotFoundException {
    // TODO

    // 数据库存在
    if (this.dbDir.exists() && this.dbDir.isDirectory() && this.dbMeta.exists() && this.dbMeta.isFile())
    {
      DBMeta meta;
      this.lock.readLock().lock();
      ObjectInputStream ois = new ObjectInputStream(new FileInputStream(this.dbMeta));
      meta = (DBMeta) ois.readObject();
      ois.close();
      this.lock.readLock().unlock();
      this.tableNames = meta.tableNames;
      int i = 0;
      for (String tbn : this.tableNames)
      {
        TableP tb = new TableP(this.databaseName, tbn);
        this.tables.put(tbn, tb);
        i++;
        if (i == Global.MAX_CACHED_TABLE_NUM)
          break;
      }
    }
    // 数据库不存在
    else if (!this.dbDir.exists() && !dbMeta.exists())
    {
      boolean flag = true;
      dbDir.mkdir();
      dbMeta.createNewFile();
      if (!dbDir.isDirectory() || !dbMeta.isFile())
          throw new FileCreateFailedException();
    }
    // 文件结构异常
    else
      throw new FileStructureException(this.databaseName);

    this.txManager.insertSession(this.walManager.getSessionID());
    this.walManager.recover();
  }

  public void quit() throws IOException {
    // TODO
    for (TableP tb : this.tables.values())
      tb.close();
    this.walManager.clearLog();
    this.tables.clear();
    persist();
  }

  private void addTable(String name, TableP tb) throws IOException {
    if (this.tables.size() > Global.MAX_CACHED_TABLE_NUM)
    {
      for (Map.Entry<String, TableP> e : this.tables.entrySet())
      {
        TableP otb = this.tables.remove(e.getKey());
        otb.close();
        otb = null;
        break;
      }
    }

    this.tables.put(name, tb);

  }

  public ArrayList<String> getTableNames() {
    return new ArrayList<>(this.tableNames);
  }

  public String getDatabaseName() {return databaseName;}

  public boolean tableInDB(String tbName) {return this.tableNames.contains(tbName);}

  public String getCurrentStatement() { return this.currentStatement; }

  public void setCurrentStatement(String statement) {this.currentStatement = statement;}

}
