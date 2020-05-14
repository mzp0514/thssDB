package cn.edu.thssdb.exception;

public class ColumnNameNotExistException extends RuntimeException{
	@Override
	public String getMessage() {
		return "Exception: the column name you select does not exist!";
	}
}
