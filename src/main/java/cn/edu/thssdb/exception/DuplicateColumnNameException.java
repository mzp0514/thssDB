package cn.edu.thssdb.exception;

public class DuplicateColumnNameException extends RuntimeException {
	@Override
	public String getMessage() {
		return "Exception: creating new table caused duplicated column names!";
	}
}
