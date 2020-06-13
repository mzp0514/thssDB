package cn.edu.thssdb.utils;

import java.util.ArrayList;
import java.util.Arrays;

public class Global {
  public static int fanout = 129;

  public static int SUCCESS_CODE = 0;
  public static int FAILURE_CODE = -1;

  public static String DEFAULT_SERVER_HOST = "127.0.0.1";
  public static int DEFAULT_SERVER_PORT = 6667;

  public static String CLI_PREFIX = "ThssDB>";
  public static final String SHOW_TIME = "show time;";
  public static final String QUIT = "quit;";
  public static final String CONNECT_PREFIX = "connect";

  public static final ArrayList<String> STATEMENT_PREFIX = new ArrayList<String>(Arrays.asList("create", "drop", "insert", "delete", "update", "select", "show",
          "begin", "commit", "rollback", "checkpoint", "use"));

  public enum STATE_TYPE { INSERT, UPDATE, DELETE }

  public static int PAGE_SIZE = 4096;
  public static int PAGE_HEADER_SIZE = 16;

  public static int INT_SIZE = 4;
  public static int LONG_SIZE = 8;
  public static int DOUBLE_SIZE = 8;
  public static int FLOAT_SIZE = 4;
  public static int BYTE_SIZE = 1;

  public static final String S_URL_INTERNAL = "jdbc:default:connection";

  public static String resize(String s, int size){
    byte[] bytes = s.getBytes();
    for(int i = bytes.length; i < size; i++){
      s += " ";
    }
    return s;
  }

  public static int MAX_CACHED_TABLE_NUM = 12;

}
