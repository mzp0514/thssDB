package cn.edu.thssdb.client;

import cn.edu.thssdb.rpc.thrift.*;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.utils.Global;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Scanner;

public class Client {

  private static final Logger logger = LoggerFactory.getLogger(Client.class);

  static final String HOST_ARGS = "h";
  static final String HOST_NAME = "host";

  static final String HELP_ARGS = "help";
  static final String HELP_NAME = "help";

  static final String PORT_ARGS = "p";
  static final String PORT_NAME = "port";

  private static final PrintStream SCREEN_PRINTER = new PrintStream(System.out);
  private static final Scanner SCANNER = new Scanner(System.in);

  private static TTransport transport;
  private static TProtocol protocol;
  private static IService.Client client;
  private static CommandLine commandLine;

  private static long sessionID;

  public static void main(String[] args) {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        disConnect(sessionID);
        logger.info("Disconnected");
      } catch (Exception e) {
        e.printStackTrace();
      }
    }));
    commandLine = parseCmd(args);
    if (commandLine.hasOption(HELP_ARGS)) {
      showHelp();
      return;
    }
    try {
      echoStarting();
      String host = commandLine.getOptionValue(HOST_ARGS, Global.DEFAULT_SERVER_HOST);
      int port = Integer.parseInt(commandLine.getOptionValue(PORT_ARGS, String.valueOf(Global.DEFAULT_SERVER_PORT)));
      transport = new TSocket(host, port);
      transport.open();
      protocol = new TBinaryProtocol(transport);
      client = new IService.Client(protocol);
      boolean open = true;
      while (true) {
        print(Global.CLI_PREFIX);
        String msg = SCANNER.nextLine();
        long startTime = System.currentTimeMillis();
        switch (msg.trim()) {
          case Global.SHOW_TIME:
            getTime();
            break;
          case Global.QUIT:
            open = false;
            disConnect(sessionID);
            break;
          default:
            String command = msg.trim();
            if(msg.trim().endsWith(";"))
            {
              command = command.substring(0, msg.trim().length() - 1);
            }
            String prefix = command.split(" ")[0];
            if (command.toLowerCase().startsWith(Global.CONNECT_PREFIX) && command.split(" ").length == 3)
            {
                String[] strList = msg.trim().split(" ");
                String username = strList[1];
                String password = strList[2];
                connect(username, password);
                break;
            }
            else if (Global.STATEMENT_PREFIX.contains(prefix.toLowerCase()))
            {
              executeStatement(sessionID, command);
              break;
            }
            println("Invalid statements!");
            break;
        }
        long endTime = System.currentTimeMillis();
        println("It costs " + (endTime - startTime) + " ms.");
        if (!open) {
          break;
        }
      }
      transport.close();
    } catch (TTransportException e) {
      logger.error(e.getMessage());
    }
  }

  private static void getTime() {
    GetTimeReq req = new GetTimeReq();
    try {
      println(client.getTime(req).getTime());
    } catch (TException e) {
      logger.error(e.getMessage());
    }
  }

  private static void connect(String username, String password){
    ConnectReq req = new ConnectReq(username, password);
    try {
      ConnectResp resp = client.connect(req);
      Status status = resp.getStatus();
      if (status.code == Global.SUCCESS_CODE)
      {
        println(status.getMsg());
        sessionID = resp.getSessionId();
      }
      else
      {
        println(status.getMsg());
      }
    } catch (TException e) {
      logger.error(e.getMessage());
    }
  }


  private static void executeStatement(long sessionID, String statement) {
    ExecuteStatementReq req = new ExecuteStatementReq(sessionID, statement);
    try {
      ExecuteStatementResp resp = client.executeStatement(req);
      Status status = resp.getStatus();
      if (status.getCode() == Global.SUCCESS_CODE)
      {
        println("Successfully deliver the statement to the server");
        if(resp.isHasResult()){
          println("Execution success");
          if(resp.isSetColumnsList()) {
            println(String.join("|", resp.columnsList));
          }
          if(resp.isSetRowList()){
            println(status.msg);
          }
        }
        else if (!resp.isHasResult())
        {
          println(status.msg);
        }

      }
      else
      {
        println(status.getMsg());
      }
    } catch (TException e) {
      logger.error(e.getMessage());
    }

  }

  private static void disConnect(long sessionID) {
    DisconnectReq req = new DisconnectReq(sessionID);
    try {
      DisconnectResp resp = client.disconnect(req);
      Status status = resp.getStatus();
      print(status.getMsg());
    } catch (TException e) {
      logger.error(e.getMessage());
    }
    finally {
      transport.close();
    }
  }

  static Options createOptions() {
    Options options = new Options();
    options.addOption(Option.builder(HELP_ARGS)
        .argName(HELP_NAME)
        .desc("Display help information(optional)")
        .hasArg(false)
        .required(false)
        .build()
    );
    options.addOption(Option.builder(HOST_ARGS)
        .argName(HOST_NAME)
        .desc("Host (optional, default 127.0.0.1)")
        .hasArg(false)
        .required(false)
        .build()
    );
    options.addOption(Option.builder(PORT_ARGS)
        .argName(PORT_NAME)
        .desc("Port (optional, default 6667)")
        .hasArg(false)
        .required(false)
        .build()
    );
    return options;
  }

  static CommandLine parseCmd(String[] args) {
    Options options = createOptions();
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      logger.error(e.getMessage());
      println("Invalid command line argument!");
      System.exit(-1);
    }
    return cmd;
  }

  static void showHelp() {
    // TODO
    println("DO IT YOURSELF");
  }

  static void echoStarting() {
    println("----------------------");
    println("Starting ThssDB Client");
    println("----------------------");
  }

  static void print(String msg) {
    SCREEN_PRINTER.print(msg);
  }

  static void println() {
    SCREEN_PRINTER.println();
  }

  static void println(String msg) {
    SCREEN_PRINTER.println(msg);
  }
}
