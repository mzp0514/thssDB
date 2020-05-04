package cn.edu.thssdb.exception;

public class DBNotFoundException extends RuntimeException{
    private final String dbname;

    public DBNotFoundException(String dbname)
    {
        super(dbname);
        this.dbname = dbname;
    }

    @Override
    public String getMessage() {
        return String.format("Exception: Can not found database: %s", this.dbname);
    }
}
