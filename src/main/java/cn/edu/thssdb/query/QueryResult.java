package cn.edu.thssdb.query;

import cn.edu.thssdb.exception.ColumnNameNotExistException;
import cn.edu.thssdb.exception.ImplicitColumnNameException;
import cn.edu.thssdb.exception.KeyNotExistException;
import cn.edu.thssdb.exception.TableNameNotExist;
import cn.edu.thssdb.index.BPlusTreeIteratorP;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import cn.edu.thssdb.schema.TableP;
import cn.edu.thssdb.type.ComparisonType;
import cn.edu.thssdb.type.JoinType;
import javafx.scene.control.Tab;
import javafx.util.Pair;

public class QueryResult {
  private List<TableP> queryTables;
  private ArrayList<ArrayList<Integer>> index;
  private ArrayList<ArrayList<Integer>> duplicate;
  private ArrayList<ArrayList<String>> attrs;

  private TableP queryTable;
  private ArrayList<Integer> index_s;
  private ArrayList<String> attrs_s;

  public QueryResult(List<TableP> queryTables, List<String> select_columns,
                     String attr1, String attr2, JoinType type, boolean allAttr) {
    // TODO
    index = new ArrayList<>();
    duplicate = new ArrayList<>();
    attrs = new ArrayList<>();
    for(int i = 0; i < 2; i++) {
      index.add(new ArrayList<>());
      duplicate.add(new ArrayList<>());
      attrs.add(new ArrayList<>());
    }

    this.queryTables = queryTables;

    if(type == JoinType.NATURAL_JOIN) {

      ArrayList<Column> col1 = queryTables.get(0).columns, col2 = queryTables.get(1).columns;
      int size1 = col1.size(), size2 = col2.size();
      for (int i = 0; i < size1; i++) {
        for (int j = 0; j < size2; j++) {
          if (col1.get(i).getName().equals(col2.get(j).getName())) {
            duplicate.get(0).add(i);
            duplicate.get(1).add(j);
            break;
          }
        }
      }
    }
    else if(type == JoinType.INNER_JOIN){
      Pair<Integer, Integer> p1 = findColumn(attr1);
      Pair<Integer, Integer> p2 = findColumn(attr2);
      duplicate.get(p1.getKey()).add(p1.getValue());
      duplicate.get(p2.getKey()).add(p2.getValue());
    }

    if(!allAttr) {
      for (int i = 0; i < select_columns.size(); i++) {
        Pair<Integer, Integer> p = findColumn(select_columns.get(i));
        index.get(p.getKey()).add(p.getValue());
        attrs.get(p.getKey()).add(select_columns.get(i));
      }
    }
    else{

      ArrayList<Column> col1 = queryTables.get(0).columns, col2 = queryTables.get(1).columns;
      int size1 = col1.size(), size2 = col2.size();
      for(int i = 0; i < size1; i++){
        index.get(0).add(i);
        attrs.get(0).add(col1.get(i).getName());
      }
      for(int i = 0; i < size2; i++){
        if(!duplicate.get(1).contains(i) || type == JoinType.INNER_JOIN) {
          index.get(1).add(i);
          attrs.get(1).add(col2.get(i).getName());
        }
      }

    }

  }

  public QueryResult(TableP queryTable, List<String> select_columns, boolean allAttr){
    this.queryTable = queryTable;
    index_s = new ArrayList<>();
    attrs_s = new ArrayList<>();
    if(allAttr){
      for(int i = 0; i < queryTable.columns.size(); i++){
        attrs_s.add(queryTable.columns.get(i).getName());
        index_s.add(i);
      }
    }
    else{
      for(int i = 0; i < select_columns.size(); i++){
        attrs_s.add(select_columns.get(i));
        index_s.add(findColumn_s(select_columns.get(i)));
      }
    }
  }

  public ArrayList<Row> queryAll() throws IOException {
    ArrayList<Row> res = new ArrayList<>();
    BPlusTreeIteratorP it = queryTables.get(0).index.iterator();
    while(it.hasNext()){
      Row temp = it.next().getValue();
      ArrayList<Object> vals = new ArrayList<>();
      ArrayList<Entry> entries = temp.getEntries();
      int size = duplicate.get(0).size();
      for(int u = 0; u < size; u++){
        vals.add(entries.get(duplicate.get(0).get(u)));
      }
      ArrayList<Row> tmp2 = queryTables.get(1).match(duplicate.get(1), vals);
      for(int j = 0; j < tmp2.size(); j++){
        res.add(combineRow(new ArrayList<Row>(Arrays.asList(temp, tmp2.get(j)))));
      }
    }
    return res;
  }

  public ArrayList<Row> queryAll_s() throws IOException {
    ArrayList<Row> res = new ArrayList<>();
    BPlusTreeIteratorP it = queryTable.index.iterator();
    while(it.hasNext()){
      Row temp = it.next().getValue();
      res.add(combineRow_s(temp));
    }
    return res;
  }


  public ArrayList<Row> queryVal(String attrName, ComparisonType comp, Object attrValue) throws IOException {
    Pair<Integer, Integer> p = findColumn(attrName);
    ArrayList<Row> tmp = queryTables.get(p.getKey()).select(p.getValue(), attrValue, comp);
    ArrayList<Row> res = new ArrayList<>();
    for(int i = 0; i < tmp.size(); i++) {
      ArrayList<Object> vals = new ArrayList<>();
      ArrayList<Entry> entries = tmp.get(i).getEntries();
      int size = duplicate.get(p.getKey()).size();
      for(int u = 0; u < size; u++){
        vals.add(entries.get(duplicate.get(p.getKey()).get(u)));
      }
      ArrayList<Row> tmp2 = queryTables.get(1 - p.getKey()).match(duplicate.get(1 - p.getKey()), vals);
      for(int j = 0; j < tmp2.size(); j++){
        res.add(combineRow(new ArrayList<Row>(Arrays.asList(tmp.get(i), tmp2.get(j)))));
      }
    }
    return res;
  }

  public ArrayList<Row> queryVal_s(String attrName, ComparisonType comp, Object attrValue) throws IOException {
    ArrayList<Row> tmp = queryTable.select(findColumn_s(attrName), attrValue, comp);
    ArrayList<Row> res = new ArrayList<>();
    for(int i = 0; i < tmp.size(); i++) {
      res.add(combineRow_s(tmp.get(i)));
    }
    return res;
  }

  public ArrayList<String> getAttrNames(){
    ArrayList<String> res = new ArrayList<>();
    res.addAll(attrs.get(0));
    res.addAll(attrs.get(1));
    return res;
  }

  public ArrayList<String> getAttrNames_s(){
    return attrs_s;
  }


  private Pair<Integer, Integer> findColumn(String attrName){
    String[] s = attrName.split("\\.");
    int tb = 0, attr = 0;
    if(s.length == 2) {
      if(queryTables.get(0).tableName.equals(s[0])){
        tb = 0;
      }
      else if(queryTables.get(1).tableName.equals(s[0])){
        tb = 1;
      }
      else{
        throw new TableNameNotExist();
      }

      attr = queryTables.get(tb).getAttrIndex(s[1]);
      if(attr == -1){
        throw new ColumnNameNotExistException();
      }

    }
    else{
      int id1 = queryTables.get(0).getAttrIndex(s[0]), id2 = queryTables.get(1).getAttrIndex(s[0]);
      if(id1 == -1 && id2 == -1){
        throw new KeyNotExistException();
      }
      else if(id1 != -1 && id2 != -1){
        throw new ImplicitColumnNameException();
      }
      else if(id1 != -1){
        attr = id1;
      }
      else{
        attr = id2;
      }
    }
    return new Pair<>(tb, attr);
  }

  private int findColumn_s(String attrName){
    int id = queryTable.getAttrIndex(attrName);
    if(id != -1){
      return id;
    }
    else{
      throw new ColumnNameNotExistException();
    }
  }

  public Row combineRow(ArrayList<Row> rows) {
    ArrayList<Entry> entries = new ArrayList<>();
    for(int i = 0; i < 2; i++){
      for(int j = 0; j < index.get(i).size(); j++) {
        entries.add(rows.get(i).getEntries().get(index.get(i).get(j)));
      }
    }
    Row res = new Row();
    res.appendEntries(entries);
    return res;
  }

  public Row combineRow_s(Row row) {
    ArrayList<Entry> entries = new ArrayList<>();

    for(int j = 0; j < index_s.size(); j++) {
      entries.add(row.getEntries().get(index_s.get(j)));
    }

    Row res = new Row();
    res.appendEntries(entries);
    return res;
  }

}