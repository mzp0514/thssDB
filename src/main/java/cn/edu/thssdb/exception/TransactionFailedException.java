package cn.edu.thssdb.exception;

public class TransactionFailedException  extends RuntimeException{
    @Override
    public String getMessage() {
        return "Exception: something wrong with transaction statement.";
    }
}
