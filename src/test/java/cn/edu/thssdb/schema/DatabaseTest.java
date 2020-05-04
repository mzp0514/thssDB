package cn.edu.thssdb.schema;

import cn.edu.thssdb.schema.Database;

import cn.edu.thssdb.type.ColumnType;
import org.junit.Before;
import org.junit.Test;

import javax.xml.ws.RespectBinding;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import static org.junit.Assert.*;


public class DatabaseTest {
    private static String dbName = "dbtest";
    private static String tbName = "tb";
    private static String tbName1 = "tb1";
    private static String tbName2 = "tb2";
    private static String tbName3 = "tb3";
    private Database db;

    private static ArrayList<String> tbList;

    Column[] columns;

    @Before
    public void setUp() {
        tbList = new ArrayList<>();
        tbList.add(tbName);
        tbList.add(tbName1);
        tbList.add(tbName2);
        tbList.add(tbName3);
    }


    @Test
    public void createDBTest() throws IOException, ClassNotFoundException {
        File dbDir = new File("data/" + dbName + "/");
        File dbMeta = new File("data/" + dbName + "/" + dbName + ".dbmeta");
        assertTrue(!dbDir.exists());
        db = new Database(dbName);
        assertTrue(dbDir.isDirectory());
        assertTrue(dbMeta.isFile());
        db.quit();
    }

    @Test
    public void createTBTest() throws IOException, ClassNotFoundException {
        db = new Database(dbName);
        columns = new Column[5];
//		String name, ColumnType type, int primary, boolean notNull, int maxLength
        columns[0] = new Column("id", ColumnType.INT, 1, true , 0);
        columns[1] = new Column("name", ColumnType.STRING, 0, true , 3);
        columns[2] = new Column("class", ColumnType.STRING, 0, true , 2);
        columns[3] = new Column("height", ColumnType.DOUBLE, 0, true , 0);
        columns[4] = new Column("weight", ColumnType.DOUBLE, 0, true , 0);
        db.create(tbName, columns);
        db.create(tbName1, columns);
        db.create(tbName2, columns);
        db.create(tbName3, columns);
        db.quit();
        db = null;
        db = new Database(dbName);
        ArrayList<String> tbs = db.getTableNames();
        assertEquals(tbs.size(), 4);
        assertTrue(tbs.containsAll(tbList));
        db.quit();
        /*Then, run the TableP Test for tb, all passed*/
    }

    /*Please Run this Test AFTER createTableTest*/
    @Test
    public void dropTableTest() throws IOException, ClassNotFoundException {
        db = new Database(dbName);
        db.drop(tbName1);
        ArrayList<String> tbs = db.getTableNames();
        assertFalse(tbs.contains(tbName1));
        FilenameFilter filter = new FilenameFilter() {
            @Override
            public boolean accept(File pathname, String name) {
                return name.startsWith(tbName1);
            }
        };
        File dbDir = new File("data/" + dbName + '/');
        File[] files = dbDir.listFiles(filter);
        assertEquals(files.length, 0);
        db.quit();
    }

}
