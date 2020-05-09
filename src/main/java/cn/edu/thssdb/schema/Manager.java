package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.*;
import cn.edu.thssdb.server.ThssDB;
import cn.edu.thssdb.utils.Global;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Manager {
  private static String defaultDB = "PUBLIC";
  private String filePath;
  private String curDBName;
  private HashSet<String> databaseNames;
  private Database curDB;
  private File dbManagerDir;
  private File dbManagerMeta;
  private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  public static Manager getInstance() {
    return Manager.ManagerHolder.INSTANCE;
  }

  public Manager() throws IOException, ClassNotFoundException {
    // TODO
    this.filePath = new String("data/");
    this.databaseNames = new HashSet<>();
    this.dbManagerDir = new File(this.filePath);
    this.dbManagerMeta = new File(this.filePath + "/db.meta");
    recover();
    createDatabaseIfNotExists(defaultDB);
    this.curDB = new Database(defaultDB);
    this.curDBName = defaultDB;
  }

  public void createDatabaseIfNotExists(String dbName) throws IOException, ClassNotFoundException {
    // TODO
    if (!this.databaseNames.contains(dbName))
    {
      Database db = new Database(dbName);
      this.databaseNames.add(dbName);
      persist();
    }
  }

  public void deleteDatabase(String dbName) throws IOException {
    // TODO
    if (dbName.equals(defaultDB) || dbName.equals(curDBName))
      throw new XDBException("Error: You can not delete the default or current database!");
    File dbDir = new File(this.filePath + dbName + "/");
    if (!this.databaseNames.contains(dbName) || !dbDir.isDirectory())
      throw new KeyNotExistException();
    this.databaseNames.remove(dbName);
    File[] files = dbDir.listFiles();
    for (File f : files)
      f.delete();
    dbDir.delete();
    if (dbDir.isDirectory())
      throw new FileDeleteFailedException();
    persist();
  }

  public void switchDatabase(String dbName) throws IOException, ClassNotFoundException {
    // TODO
    if (dbName.equals(this.curDBName))
      throw new XDBException("Error: The chosen database is already loaded!");
    if (!this.databaseNames.contains(dbName))
      throw new DBNotFoundException(dbName);
    this.curDB.quit();
    this.curDB = new Database(dbName);
    this.curDBName = dbName;
  }



  public String getCurDBName() {
    return curDBName;
  }

  public Database getCurDB() {
    return curDB;
  }

  public ArrayList<String> getDBNames() {
    return new ArrayList<>(this.databaseNames);
  }

  private static class ManagerHolder {
    private static Manager INSTANCE = null;

    static {
      try {
        INSTANCE = new Manager();
      } catch (IOException | ClassNotFoundException e) {
        e.printStackTrace();
      }
    }

    private ManagerHolder() {

    }
  }

  private void recover() throws IOException, ClassNotFoundException {

    if (!this.dbManagerDir.exists() || !this.dbManagerMeta.exists())
    {
      this.dbManagerDir.mkdir();
      this.dbManagerMeta.createNewFile();
      if (!this.dbManagerDir.isDirectory() || !this.dbManagerMeta.isFile())
        throw new FileCreateFailedException();
    }

    else
    {
      if (this.dbManagerDir.exists() && this.dbManagerDir.isDirectory() && this.dbManagerMeta.exists() && this.dbManagerMeta.isFile()) {
        this.lock.readLock().lock();
        ObjectInputStream ois = new ObjectInputStream(new FileInputStream(this.dbManagerMeta));
        this.databaseNames = (HashSet<String>) ois.readObject();
        ois.close();
        this.lock.readLock().unlock();
      }
      else
        throw new FileStructureException("DataBaseManager");
    }



  }

  private void persist() throws IOException {
    this.lock.writeLock().lock();
    FileOutputStream fs1 = new FileOutputStream(this.dbManagerMeta);
    ObjectOutputStream os1 =  new ObjectOutputStream(fs1);
    os1.writeObject(this.databaseNames);
    os1.close();
    fs1.close();
    this.lock.writeLock().unlock();
  }
}
