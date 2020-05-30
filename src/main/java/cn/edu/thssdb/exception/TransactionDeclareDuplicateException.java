package cn.edu.thssdb.exception;

public class TransactionDeclareDuplicateException extends RuntimeException{
    @Override
    public String getMessage() {
        return "Exception: the same transaction statement already exists.";
    }
}
