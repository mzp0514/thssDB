package cn.edu.thssdb.schema;

import cn.edu.thssdb.type.ColumnType;
import org.junit.Before;
import org.junit.Test;


import javax.xml.bind.annotation.XmlAnyElement;
import javax.xml.crypto.Data;
import javax.xml.ws.RespectBinding;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import static org.junit.Assert.*;

public class ManagerTest {

    private Manager manager;
    private String curDBName;
    private Database curDB;


    @Test
    public void runManagerTest() throws IOException, ClassNotFoundException {
        File defaultDBFile = new File("data/PUBLIC/PUBLIC.dbmeta");
        manager = new Manager();
        assertTrue(defaultDBFile.isFile());
    }

    @Test
    public void createDBTest() throws IOException, ClassNotFoundException {
        manager = new Manager();
        ArrayList<String> list = manager.getDBNames();
        assertFalse(list.contains("dbtest"));
        File dbFile = new File("data/dbtest/dbtest.dbmeta");
        manager.createDatabaseIfNotExists("dbtest");
        manager.createDatabaseIfNotExists("tb");
        list = manager.getDBNames();
        assertTrue(list.contains("dbtest"));
        assertTrue(dbFile.isFile());

    }

    @Test
    public void resumeManagerTest() throws IOException, ClassNotFoundException {
        manager = new Manager();
        ArrayList<String> list = manager.getDBNames();
        assertTrue(list.contains("dbtest"));
        assertTrue(list.contains("tb"));
    }

    @Test
    public void deleteDBTest() throws IOException, ClassNotFoundException {
        manager = new Manager();
        ArrayList<String> list = manager.getDBNames();
        assertTrue(list.contains("tb"));
        manager.deleteDatabase("tb");
        manager = null;
        manager = new Manager();
        list = manager.getDBNames();
        assertFalse(list.contains("tb"));
    }

//    @Test
//    public void switchDBTest() throws IOException, ClassNotFoundException {
//        String dname = "PUBLIC";
//        String nname = "dbtest";
//        manager = new Manager();
//        curDBName = manager.getCurDBName();
//        curDB = manager.getCurDB();
//        assertEquals(curDBName, dname);
//        assertEquals(curDB.getDatabaseName(), dname);
//        manager.switchDatabase("dbtest");
//        curDBName = manager.getCurDBName();
//        curDB = manager.getCurDB();
//        assertEquals(curDBName, nname);
//        assertEquals(curDB.getDatabaseName(), nname);
//
//    }

}
