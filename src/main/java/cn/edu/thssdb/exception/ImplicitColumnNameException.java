package cn.edu.thssdb.exception;

public class ImplicitColumnNameException extends RuntimeException {
	@Override
	public String getMessage() {
		return "Exception: the column name you select is ambiguous!";
	}
}
