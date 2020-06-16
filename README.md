# thssDB

## 登录与注销

* 连接数据库
  * 语法为：
```sql
CONNECT username password
```
  * 默认用户名、密码分别为为username password（当前仅支持该用户）
  * 登录之后，用户默认的数据库为public
* 断开连接
  * 语句为：
```sql
DISCONNECT
```
## 数据库相关操作

* 创建数据库
  * 语法为：
```sql
CREATE DATABASE databaseName
```
  * 若数据库已经存在，会进行相应的创建失败提示
* 切换数据库
  * 语法为：
```sql
USE databaseName
```
  * 若数据库不存在，会进行相应的切换失败提示
* 删除数据库
  * 语法为：
```sql
DELETE databaseName
```
  * 若删除的数据库不存在或是正在被其他用户使用，会进行相应的删除失败提示
## 表相关操作

* 创建表
  * 语法为：
```sql
CREATE TABLE tableName(attrName1 Type1, attrName2 Type2,…, attrNameN TypeN NOT NULL, PRIMARY KEY(attrName1))
```
  * 示例：
```sql
create table instructor (i_id String(5), i_name String(20) not null, dept_name String(20), salary Float, primary key(i_id))
```
  * 支持的数据类型有：Int，Long，Float，Double，String（需指定最大长度）
  * 主键仅在某一列定义，若定义多个会有错误提示
  * 对于没有主键定义，属性、类型缺失，表名重复的情况，会有异常提示。
* 删除表
  * 语法为：
```sql
DROP TABLE tableName
```
  * 清空该表的缓存，并删除硬盘上的持久化数据
  * 若表不存在，则提示错误
* 展示表信息
  * 语法为：
```sql
SHOW TABLE tableName
```
  * 展示格式: 每一个Column的信息用一行显示，若类型为String，则一并展示最大长度。
```plain
 ColName: name, Type: type(maxLength)
```

## 记录的增删改查

* 记录插入语句
  * 语法为：
```sql
INSERT INTO tableName[(attrName1, attrName2,…, attrNameN)] VALUES (attrValue1, attrValue2,…, attrValueN)
```
  * 示例：
```sql
insert into instructor values('63395', 'McKinnon', 'Cybernetics', 94333.99)
insert into instructor(i_id, i_name, dept_name, salary) values('96895', 'Mird', 'Marketing', 119921.41)
insert into instructor(i_id, i_name) values('78699', 'Pingr')
```
  * 若在表名后不指定列的顺序，则使用创建表时默认的列顺序。
  * 若列的数目与每个条目的长度不等、字符串格式、长度不正确，表不存在等情况，均会提示异常。
  * 支持一个语句中同时插入多行，若有一行不合法，则不进行插入操作
* 记录删除语句
  * 语法为：
```sql
DELETE FROM tableName [WHERE attrName = attValue]
```
  * 示例：
```sql
delete from instructor where dept_name = 'Marketing'
delete from instructor
```
  * 若没有where子句，则会删除表中的所有内容，仅保留一张空表
  * where子句的 "=" 可以替换为"<", ">", "<=", ">=", "<>"，至多涉及一个比较
  * 对于不存在数据表、删除内容为空、不支持的语法、错误的column name均会进行异常提示
* 记录修改语句
  * 语法为：
```sql
UPDATE tableName SET attrName = attrValue [WHERE attrName = attrValue]
```
  * 示例：
```sql
update student set s_name = 'Dell' where s_id = '76291'
update student set s_name = 'Dell'
```
  * 若没有where子句，则会更新表中所有row的相关内容
  * where子句的 "=" 可以替换为"<", ">", "<=", ">=", "<>"，至多涉及一个比较
  * 对于不存在数据表、不支持的语法、错误的column name均会进行异常提示
* 记录查询语句
  * 支持的语句有：
```sql
SELECT * FROM tableName [ WHERE attrName1 = attrValue ]  
       
SELECT attrName1, attrName2, … attrNameN FROM tableName [ WHERE attrName1 = attrValue ]  
       
SELECT * FROM tableName1 JOIN tableName2 ON tableName1.attrName1 = tableName2.attrName2 [ WHERE  tableName1.tattrName1 = attrValue ] 
       
SELECT [tableName1.]AttrName1, [tableName1.]AttrName2, ... FROM tableName1 JOIN tableName2 ON tableName1.attrName1 = tableName2.attrName2 [ WHERE  [tableName1.]tattrName1 = attrValue ]   
       
SELECT * FROM  tableName1 NATURAL JOIN tableName2 [ WHERE  [tableName1.]attrName1 = attrValue ] 
       
SELECT [tableName1.]AttrName1, [tableName1.]AttrName2, ... FROM  tableName1 NATURAL JOIN tableName2 [ WHERE [tableName1.]attrName1 = attrValue ] 
```
  * 示例
```sql
select * from instructor 
select * from instructor where dept_name = 'Marketing'
       
select i_name, salary from instructor where dept_name = 'Marketing'
select course_id, title from course join department on course.dept_name = department.dept_name where building <> 'Palmer';
       
select course_id, course.dept_name from course natural join department where building <> 'Palmer';
```
  * 若没有where子句，则会选择表中的所有条目
  * 二表查询中，不引起歧义的AttrName（非两个表中重复的Column name）可不加tableName
  * where子句的 "=" 可以替换为"<", ">", "<=", ">=", "<>"，至多涉及一个比较
  * 对于不存在数据表、不支持的语法、错误的column name、引发歧义的column name均会进行异常提示
  * 查询输出格式：每个row一行，不同column以"，"隔开。如：
```plain
i_id, i_name, dept_name, salary
14365, Lembr, Accounting, 32241.56
15347, Bawa, Athletics, 72140.88
16807, Yazdi, Athletics, 98333.65
19368, Wieland, Pol. Sci., 124651.41
```
## 事务与恢复

* 事务开启：
  * 语句为：
```sql
begin transaction
```
  * 开启事务后，不能使用数据表、数据库级别的增删以及切换操作
  * 事务内的增删操作会锁定用到的数据表，其他客户端将无法对这个数据表进行操作
* 事务完成：
  * 语句为：
```sql
commit transaction
```
  * 执行完成语句后，会将当前事务操作后的数据库保存在内存中，并进行log记录
* 事务回滚：
  * 语句为：
```sql
rollback transaction
```
  * 执行完回滚操作后，会回滚到事务开始时或最后一个检查点(若事务中使用了checkpoint语句)时数据库的状态
* 检查点：
  * 语句为：
```sql
checkpoint
```
  * 在事务中执行检查点操作后，之前的操作会自动commit，并进行持久化
  * 事务中执行完检查点操作后再次执行回滚操作，会回滚到最后一个检查点时的数据库状态
  * 在事务外执行检查点语句后，会将数据库以当前状态进行持久化

