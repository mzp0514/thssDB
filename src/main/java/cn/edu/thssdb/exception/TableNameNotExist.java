package cn.edu.thssdb.exception;

public class TableNameNotExist extends RuntimeException {
	@Override
	public String getMessage() {
		return "Exception: table not exist!";
	}
}
