package cn.edu.thssdb.exception;

public class XDBException extends RuntimeException {

    public XDBException(String dbname) {
        super(dbname);
    }
}
