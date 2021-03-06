package cn.edu.thssdb.schema;

import cn.edu.thssdb.parser.ParseErrorListener;
import cn.edu.thssdb.parser.SQLLexer;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.parser.SQLVisitorStatement;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.utils.Global;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class DBLog  implements Serializable {
    public List<String> statements;

    public DBLog(List<String> statement)
    {
        this.statements = statement;
    }
}

public class WALManager {
    private Database db;
    private String dbName;
    private String filePath;
    private File dbLog;
    private List<String> statements;
    private int sessionID;
    ReentrantReadWriteLock lock;

    public WALManager(Database db, String filePath){
        this.db = db;
        this.dbName = db.getDatabaseName();
        this.filePath = filePath;
        this.dbLog = new File(filePath + this.dbName + ".log");
        this.statements = new ArrayList<>();
        this.sessionID = 0;
        this.lock = new ReentrantReadWriteLock();
    }

    public void recover() throws IOException, ClassNotFoundException {
        if (this.dbLog.exists() && this.dbLog.isFile()){
            DBLog log;
            this.lock.readLock().lock();
            FileInputStream fs = new FileInputStream(this.dbLog);
            if (fs.available() == 0) {
                this.lock.readLock().unlock();
                return;
            }

            ObjectInputStream ois = new ObjectInputStream(fs);
            log = (DBLog) ois.readObject();
            ois.close();
            this.lock.readLock().unlock();

            QueryResult res = new QueryResult("empty");
            for (String statement : log.statements){
                try {
                    SQLLexer lexer = new SQLLexer(CharStreams.fromString(statement));
                    lexer.removeErrorListeners();
                    lexer.addErrorListener(new ParseErrorListener());
                    SQLParser parser = new SQLParser(new CommonTokenStream(lexer));
                    parser.removeErrorListeners();
                    parser.addErrorListener(new ParseErrorListener());
                    SQLParser.Sql_stmtContext stmt = parser.sql_stmt();
                    SQLVisitorStatement visitor = new SQLVisitorStatement(this.db, this.sessionID);
                    res = visitor.visit(stmt);
                } catch (Exception e) {
                    //TODO 抓取exception并显示
                    int k = -1;
                }
            }
            this.db.txManager.persistTable(this.sessionID);

            boolean result = clearLog();
            if (!result) {
                //TODO 抓取exception并显示
                clearLog();
            }
        } else {
            this.lock.writeLock().lock();
            this.dbLog.createNewFile();
            this.lock.writeLock().unlock();
        }
    }

    public void addStatement(String statement){
        this.statements.add(statement);
    }

    public void addStatement(List<String> statements){
        this.statements.addAll(statements);
    }

    public void persist() throws IOException {
        this.lock.writeLock().lock();

        DBLog log = new DBLog(this.statements);
        FileOutputStream fs = new FileOutputStream(this.dbLog, true);
        ObjectOutputStream os =  new ObjectOutputStream(fs);
        os.writeObject(log);
        os.close();
        fs.close();

        this.statements.clear();
        this.lock.writeLock().unlock();
    }

    public void persist(String statement) throws IOException {
        this.lock.writeLock().lock();
        this.statements.add(statement);

        DBLog log = new DBLog(this.statements);
        FileOutputStream fs = new FileOutputStream(this.dbLog, true);
        ObjectOutputStream os =  new ObjectOutputStream(fs);
        os.writeObject(log);
        os.close();
        fs.close();

        this.statements.clear();
        this.lock.writeLock().unlock();
    }

    public boolean clearLog() throws IOException {
        this.lock.writeLock().lock();
        this.statements.clear();
        boolean result = false;
        while(!result) {
            System.gc();
            result = this.dbLog.delete();
        }
        result = this.dbLog.createNewFile();
        this.lock.writeLock().unlock();
        return result;
    }

    public void rollBackClearStatement() {
        this.statements.clear();
    }

    public int getSessionID() {return this.sessionID;}

}
