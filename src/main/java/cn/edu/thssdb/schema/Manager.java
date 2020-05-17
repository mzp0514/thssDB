package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.*;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.locks.ReentrantReadWriteLock;


class ManagerMeta  implements Serializable {
  public HashSet<String> databaseNames;
  public HashMap<String, String> userInfo;

  public ManagerMeta(HashSet<String> names, HashMap<String, String> userInfo)
  {
    this.databaseNames = names;
    this.userInfo = userInfo;
  }
}

public class Manager {
  private static String defaultDB = "PUBLIC";
  private String filePath;
  private HashSet<String> databaseNames;
  public HashMap<String, String> userInfo;
  private HashMap<Long, String> sessionDBMap;     // key: sessionID, Value: curDBName
  private HashMap<String, Database> cachedDB;
  private File dbManagerDir;
  private File dbManagerMeta;
  private Database curDB;
  private String curDBName;
  private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  public static Manager getInstance() {
    return Manager.ManagerHolder.INSTANCE;
  }

  public Manager() throws IOException, ClassNotFoundException {
    // TODO
    this.filePath = new String("data/");
    this.databaseNames = new HashSet<>();
    this.sessionDBMap = new HashMap<>();
    this.cachedDB = new HashMap<>();
    this.dbManagerDir = new File(this.filePath);
    this.dbManagerMeta = new File(this.filePath + "/db.meta");
    recover();
    createDatabaseIfNotExists(defaultDB);
    this.curDB = new Database(defaultDB);
    this.curDBName = defaultDB;
    this.cachedDB.put(defaultDB, this.curDB);
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


  public long authUser(String username, String password) {
    if (password.equals(this.userInfo.get(username)))
    {
      Random random = new Random();
      long sessionID = random.nextLong();
      while (this.sessionDBMap.containsKey(sessionID) || sessionID == 1)
        sessionID = random.nextLong();
      this.sessionDBMap.put(sessionID, defaultDB);
      return sessionID;
    }
    else
      return -1;
  }


  public boolean authSession(long sessionID) {
    return this.sessionDBMap.containsKey(sessionID);
  }


  public Database getCurDB() {
    return curDB;
  }

  public ArrayList<String> getDBNames() {
    return new ArrayList<>(this.databaseNames);
  }

  private static class ManagerHolder {
    private static Manager INSTANCE;

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
      userInfo = new HashMap<>();
      userInfo.put("admin", "admin");
      if (!this.dbManagerDir.isDirectory() || !this.dbManagerMeta.isFile())
        throw new FileCreateFailedException();
    }

    else
    {
      if (this.dbManagerDir.exists() && this.dbManagerDir.isDirectory() && this.dbManagerMeta.exists() && this.dbManagerMeta.isFile()) {
        ManagerMeta meta;
        this.lock.readLock().lock();
        ObjectInputStream ois = new ObjectInputStream(new FileInputStream(this.dbManagerMeta));
        meta = (ManagerMeta) ois.readObject();
        ois.close();
        this.lock.readLock().unlock();
        this.databaseNames = meta.databaseNames;
        this.userInfo = meta.userInfo;
      }
      else
        throw new FileStructureException("DataBaseManager");
    }



  }

  private void persist() throws IOException {
    ManagerMeta meta = new ManagerMeta(this.databaseNames, this.userInfo);
    this.lock.writeLock().lock();
    FileOutputStream fs1 = new FileOutputStream(this.dbManagerMeta);
    ObjectOutputStream os1 =  new ObjectOutputStream(fs1);
    os1.writeObject(meta);
    os1.close();
    fs1.close();
    this.lock.writeLock().unlock();
  }

  public void close() throws IOException {
    this.curDB.quit();
  }
}


