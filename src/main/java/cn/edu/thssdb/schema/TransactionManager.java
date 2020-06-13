package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.TransactionDeclareDuplicateException;
import cn.edu.thssdb.exception.TransactionFailedException;
import cn.edu.thssdb.utils.Global;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TransactionManager {

    private Map<Long, Boolean> sessionsStatus;
    private Database db;

    ReentrantReadWriteLock lock      = new ReentrantReadWriteLock();
    ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();


    public TransactionManager(Database db){
        this.sessionsStatus = new HashMap<>();
        this.db = db;
    }

    public boolean getTransactionState(long ID){
        return sessionsStatus.get(ID);
    }

    public synchronized void insertSession(long ID) {
        writeLock.lock();

        sessionsStatus.put(ID, false);

        writeLock.unlock();
    }

    public synchronized void beginTransaction(long sessionID) {
        if (sessionsStatus.get(sessionID)) {
            throw new TransactionDeclareDuplicateException();
        } else {
            sessionsStatus.put(sessionID, true);
        }
    }

    public synchronized void commitTransaction(long sessionID) {
        if (sessionsStatus.get(sessionID)) {
            sessionsStatus.put(sessionID, false);
            try {
                for (String id : this.db.getTableNames()) {
                    TableP table = this.db.getTable(id);
                    if (table != null) {
                        table.currentSessionID = -1;
                    }
                }
            } catch (Exception e) {
                throw new TransactionFailedException();
            }

        } else {
            throw new TransactionDeclareDuplicateException();
        }
    }

    public synchronized void rollbackTransaction(long sessionID) {
        if (sessionsStatus.get(sessionID)) {
            sessionsStatus.put(sessionID, false);

            try {
                rollback(sessionID);
            } catch (Exception e) {
                throw new TransactionFailedException();
            }

        } else {
            throw new TransactionDeclareDuplicateException();
        }
    }

    public void persistTable(long sessionID) throws IOException, ClassNotFoundException {
        for (String id : this.db.getTableNames()) {
            TableP table = this.db.getTable(id);
            if (table != null) {
                table.persist();
                table.currentSessionID = -1;
            }
        }
    }

    private void rollback(long sessionID) throws IOException, ClassNotFoundException {
        for (String id : this.db.getTableNames()) {
            TableP table = this.db.getTable(id);
            if (table != null && table.currentSessionID == sessionID) {
                //TODO
                while (table.actionType.size() != 0){
                    Global.STATE_TYPE stmt = table.actionType.pop();
                    switch (stmt){
                        case DELETE:
                            table.insert(table.rowsForActions.pop());
                            break;
                        case INSERT:
                            table.delete(table.rowsForActions.pop());
                            break;
                        case UPDATE:
                            table.delete(table.rowsForActionsAppend.pop());
                            table.insert(table.rowsForActions.pop());
                            break;
                        default:
                            break;
                    }
                }

                //table.persist();
                table.currentSessionID = -1;
            }
        }
    }
}
