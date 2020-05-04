package cn.edu.thssdb.exception;

public class FileCreateFailedException extends RuntimeException{
    @Override
    public String getMessage() {
        return "Exception: File or Dir creation failed!";
    }
}
