package cn.edu.thssdb.service;

import cn.edu.thssdb.client.Client;
import cn.edu.thssdb.parser.ParseErrorListener;
import cn.edu.thssdb.parser.SQLLexer;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.parser.SQLVisitorStatement;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.rpc.thrift.ConnectReq;
import cn.edu.thssdb.rpc.thrift.ConnectResp;
import cn.edu.thssdb.rpc.thrift.DisconnetResp;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementReq;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.rpc.thrift.GetTimeReq;
import cn.edu.thssdb.rpc.thrift.GetTimeResp;
import cn.edu.thssdb.rpc.thrift.IService;
import cn.edu.thssdb.rpc.thrift.Status;
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
  public DisconnetResp disconnect(DisconnetResp req) throws TException {
    // TODO
    return null;
  }

  @Override
  public ExecuteStatementResp executeStatement(ExecuteStatementReq req) throws TException {
    // TODO
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
        SQLLexer lexer = new SQLLexer(CharStreams.fromString(statement));
        lexer.removeErrorListeners();
        lexer.addErrorListener(new ParseErrorListener());
        SQLParser parser = new SQLParser(new CommonTokenStream(lexer));
        parser.removeErrorListeners();
        parser.addErrorListener(new ParseErrorListener());
        SQLParser.Sql_stmtContext stmt = parser.sql_stmt();
        SQLVisitorStatement visitor = new SQLVisitorStatement(manager.getCurDB(), sessionID);
        res = visitor.visit(stmt);
      } catch (Exception e) {
        logger.info(e.getMessage());
      }
      status.setCode(Global.SUCCESS_CODE);
      if (res.isReturnValue)
      {
        resp.setHasResult(true);
        resp.addToColumnsList("A");
        resp.addToColumnsList("B");
        ArrayList<String> a = new ArrayList<>();
        a.add("1");
        a.add("2");
        resp.addToRowList(a);
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
