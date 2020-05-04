package cn.edu.thssdb.exception;

public class FileDeleteFailedException extends RuntimeException{
    @Override
    public String getMessage() {
        return "Exception: file delete failed!";
    }
}