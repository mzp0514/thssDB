package cn.edu.thssdb.exception;

public class FileStructureException extends RuntimeException{
    private final String dbname;

    public FileStructureException(String dbname)
    {
        super(dbname);
        this.dbname = dbname;
    }

    @Override
    public String getMessage() {
        return String.format("Exception: Wrong File Structure on data dir, dbname: %s", this.dbname);
    }
}
