package cn.edu.thssdb.service;

import cn.edu.thssdb.client.Client;
import cn.edu.thssdb.parser.ParseErrorListener;
import cn.edu.thssdb.parser.SQLLexer;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.parser.SQLVisitorStatement;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.rpc.thrift.*;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.utils.Global;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;

public class IServiceHandler implements IService.Iface {

  private static final Logger logger = LoggerFactory.getLogger(IServiceHandler.class);


  @Override
  public GetTimeResp getTime(GetTimeReq req) throws TException {
    GetTimeResp resp = new GetTimeResp();
    resp.setTime(new Date().toString());
    resp.setStatus(new Status(Global.SUCCESS_CODE));
    return resp;
  }

  @Override
  public ConnectResp connect(ConnectReq req) throws TException {
    // TODO
    ConnectResp resp = new ConnectResp();
    String username = req.username;
    String password = req.password;
    Manager manager  = Manager.getInstance();
    long sessionID = manager.authUser(username, password);
    resp.setSessionId(sessionID);
    if (sessionID != -1)
    {
      Status status = new Status(Global.SUCCESS_CODE);
      status.msg = "Login Success!";
      manager.getCurDB().txManager.insertSession(sessionID);

      resp.setStatus(status);
    }
    else
    {
      Status status = new Status(Global.FAILURE_CODE);
      status.msg = "Wrong Username or Password!";
      resp.setStatus(status);
    }
    return resp;
  }

  @Override
  public DisconnectResp disconnect(DisconnectReq req) throws TException {
    // TODO
    DisconnectResp resp = new DisconnectResp();
    long sessionID = req.getSessionId();
    Manager manager = Manager.getInstance();
    manager.disconnect(sessionID);
    Status status = new Status(Global.SUCCESS_CODE);
    status.msg = "Bye!";
    resp.setStatus(status);
    return resp;
  }

  @Override
  public ExecuteStatementResp executeStatement(ExecuteStatementReq req) throws TException {
    ExecuteStatementResp resp = new ExecuteStatementResp();
    long sessionID = req.getSessionId();
    Manager manager = Manager.getInstance();
    Status status = new Status();
    QueryResult res = new QueryResult("empty");
    if (manager.authSession(sessionID))
    {
      String statement = req.getStatement();
      logger.info("Statement: " + statement + " received, ready to parse");

      try {
        manager.getUserDB(sessionID).setCurrentStatement(statement);
        SQLLexer lexer = new SQLLexer(CharStreams.fromString(statement));
        lexer.removeErrorListeners();
        lexer.addErrorListener(new ParseErrorListener());
        SQLParser parser = new SQLParser(new CommonTokenStream(lexer));
        parser.removeErrorListeners();
        parser.addErrorListener(new ParseErrorListener());
        SQLParser.Sql_stmtContext stmt = parser.sql_stmt();
        SQLVisitorStatement visitor = new SQLVisitorStatement(manager.getUserDB(sessionID), sessionID);
        res = visitor.visit(stmt);
      } catch (Exception e) {
        status.setCode(Global.FAILURE_CODE);
        status.setMsg(e.getMessage());
        resp.setStatus(status);
        resp.setHasResult(false);
        return resp;
      }
      status.setCode(Global.SUCCESS_CODE);
      if (res.isReturnValue)
      {
        resp.setHasResult(true);
//        resp.addToColumnsList("A");
//        resp.addToColumnsList("B");
        ArrayList<String> result = new ArrayList<>();
        res.getRowsToSelect().forEach(it -> {
          result.clear();
          result.add(it.toString());
          resp.addToRowList(result);
        });

        status.setMsg(res.getMsg());
        resp.setStatus(status);
      }
      else
      {
        resp.setHasResult(false);
        status.setMsg(res.getMsg());
        resp.setStatus(status);
      }


    }
    else
    {
      status.setCode(Global.FAILURE_CODE);
      status.setMsg("Not log in!");
      resp.setStatus(status);
    }
    return resp;
  }
}
