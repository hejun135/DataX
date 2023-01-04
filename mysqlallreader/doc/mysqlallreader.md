
# MysqlAllReader 插件文档


___



## 1 快速介绍

MysqlAllReader插件实现了从Mysql读取数据。在底层实现上，MysqlAllReader通过JDBC连接远程Mysql数据库，并执行相应的sql语句将数据从mysql库中SELECT出来。

**不同于其他关系型数据库，MysqlAllReader不支持FetchSize.**

## 2 实现原理

简而言之，MysqlAllReader通过JDBC连接器连接到远程的Mysql数据库，并根据用户配置的信息生成查询SELECT SQL语句，然后发送到远程Mysql数据库，并将该SQL执行返回结果使用DataX自定义的数据类型拼装为抽象的数据集，并传递给下游Writer处理。

对于用户配置DataBase或Table集合的信息，MysqlAllReader利用datax自身的split处理流程，将自动获取数据库下的所有表及字段信息，然后将其拼接为SQL语句发送到Mysql数据库；对于用户配置querySql信息，MysqlAllReader直接将其发送到Mysql数据库。


## 3 功能说明

### 3.1 配置样例

* 配置一个从Mysql数据库同步抽取数据到本地的作业:

```
{
  "job": {
    "setting": {
      "speed": {
        "channel": 10
      },
      "errorLimit": {
        "record": 0,
        "percentage": 0.02
      }
    },
    "content": [
      {
        "reader": {
          "name": "mysqlallreader",
          "parameter": {
            "username": "test",
            "password": "test",
            "connection": [
              {
                "jdbcUrl": ["jdbc:mysql://localhost:3306/test"],
                "database": "test"
              }
            ]
          }
        },
        "writer": {
          "name": "mysqlallwriter",
          "parameter": {
            "username": "test",
            "password": "test",
            "writeMode":"replace",
            "connection": [
              {
                "jdbcUrl": "jdbc:mysql://localhost:3306/test1",
                "database": "test1"
              }
            ]
          }
        }
      }
    ]
  }
}

```



### 3.2 参数说明

* **jdbcUrl**

	* 描述：描述的是到对端数据库的JDBC连接信息，使用JSON的数组描述，并支持一个库填写多个连接地址。之所以使用JSON数组描述连接信息，是因为阿里集团内部支持多个IP探测，如果配置了多个，MysqlReader可以依次探测ip的可连接性，直到选择一个合法的IP。如果全部连接失败，MysqlReader报错。 注意，jdbcUrl必须包含在connection配置单元中。对于阿里集团外部使用情况，JSON数组填写一个JDBC连接即可。

		jdbcUrl按照Mysql官方规范，并可以填写连接附件控制信息。具体请参看[Mysql官方文档](http://dev.mysql.com/doc/connector-j/en/connector-j-reference-configuration-properties.html)。

	* 必选：是 <br />

	* 默认值：无 <br />

* **username**

	* 描述：数据源的用户名 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **password**

	* 描述：数据源指定用户名的密码 <br />

	* 必选：是 <br />

	* 默认值：无 <br />

* **table**

	* 描述：所选取的需要同步的表。使用JSON的数组描述，因此支持多张表同时抽取。当配置为多张表时，用户自己需保证多张表是同一schema结构，MysqlAllReader不予检查表是否同一逻辑表。注意，table必须包含在connection配置单元中。<br />

	* 必选：否 <br />

	* 默认值：无 <br />

* **database**
	* 描述：需要同步的schema名称，本引擎会自动获取该schema下的所有表信息，当做需要同步的表。与参数table必填其一
  
	* 必选：否 <br />

    * 默认值：无 <br />



### 3.3 类型转换

目前MysqlReader支持大部分Mysql类型，但也存在部分个别类型没有支持的情况，请注意检查你的类型。

下面列出MysqlReader针对Mysql类型转换列表:


| DataX 内部类型| Mysql 数据类型    |
| -------- | -----  |
| Long     |int, tinyint, smallint, mediumint, int, bigint|
| Double   |float, double, decimal|
| String   |varchar, char, tinytext, text, mediumtext, longtext, year   |
| Date     |date, datetime, timestamp, time    |
| Boolean  |bit, bool   |
| Bytes    |tinyblob, mediumblob, blob, longblob, varbinary    |



请注意:

* `除上述罗列字段类型外，其他类型均不支持`。
* `tinyint(1) DataX视作为整形`。
* `year DataX视作为字符串类型`
* `bit DataX属于未定义行为`。

## 4 性能报告

### 4.1 环境准备

#### 4.1.1 数据特征
建表语句：

	CREATE TABLE `tc_biz_vertical_test_0000` (
  	`biz_order_id` bigint(20) NOT NULL COMMENT 'id',
  	`key_value` varchar(4000) NOT NULL COMMENT 'Key-value的内容',
  	`gmt_create` datetime NOT NULL COMMENT '创建时间',
  	`gmt_modified` datetime NOT NULL COMMENT '修改时间',
  	`attribute_cc` int(11) DEFAULT NULL COMMENT '防止并发修改的标志',
  	`value_type` int(11) NOT NULL DEFAULT '0' COMMENT '类型',
  	`buyer_id` bigint(20) DEFAULT NULL COMMENT 'buyerid',
  	`seller_id` bigint(20) DEFAULT NULL COMMENT 'seller_id',
  	PRIMARY KEY (`biz_order_id`,`value_type`),
  	KEY `idx_biz_vertical_gmtmodified` (`gmt_modified`)
	) ENGINE=InnoDB DEFAULT CHARSET=gbk COMMENT='tc_biz_vertical'


单行记录类似于：

	biz_order_id: 888888888
   	   key_value: ;orderIds:20148888888,2014888888813800;
  	  gmt_create: 2011-09-24 11:07:20
	gmt_modified: 2011-10-24 17:56:34
	attribute_cc: 1
  	  value_type: 3
    	buyer_id: 8888888
   	   seller_id: 1

#### 4.1.2 机器参数

* 执行DataX的机器参数为:
	1. cpu: 24核 Intel(R) Xeon(R) CPU E5-2630 0 @ 2.30GHz
	2. mem: 48GB
	3. net: 千兆双网卡
	4. disc: DataX 数据不落磁盘，不统计此项

* Mysql数据库机器参数为:
	1. cpu: 32核 Intel(R) Xeon(R) CPU E5-2650 v2 @ 2.60GHz
	2. mem: 256GB
	3. net: 千兆双网卡
	4. disc: BTWL419303E2800RGN  INTEL SSDSC2BB800G4   D2010370

#### 4.1.3 DataX jvm 参数

	-Xms1024m -Xmx1024m -XX:+HeapDumpOnOutOfMemoryError


### 4.2 测试报告

#### 4.2.1 单表测试报告


| 通道数| 是否按照主键切分| DataX速度(Rec/s)|DataX流量(MB/s)| DataX机器网卡进入流量(MB/s)|DataX机器运行负载|DB网卡流出流量(MB/s)|DB运行负载|
|--------|--------| --------|--------|--------|--------|--------|--------|
|1| 否 | 183185 | 18.11 | 29| 0.6 | 31| 0.6 |
|1| 是 | 183185 | 18.11 | 29| 0.6 | 31| 0.6 |
|4| 否 | 183185 | 18.11 | 29| 0.6 | 31| 0.6 |
|4| 是 | 329733 | 32.60 | 58| 0.8 | 60| 0.76 |
|8| 否 | 183185 | 18.11 | 29| 0.6 | 31| 0.6 |
|8| 是 | 549556 | 54.33 | 115| 1.46 | 120| 0.78 |

说明：

1. 这里的单表，主键类型为 bigint(20),范围为：190247559466810-570722244711460，从主键范围划分看，数据分布均匀。
2. 对单表如果没有安装主键切分，那么配置通道个数不会提升速度，效果与1个通道一样。


#### 4.2.2 分表测试报告(2个分库，每个分库16张分表，共计32张分表)


| 通道数| DataX速度(Rec/s)|DataX流量(MB/s)| DataX机器网卡进入流量(MB/s)|DataX机器运行负载|DB网卡流出流量(MB/s)|DB运行负载|
|--------| --------|--------|--------|--------|--------|--------|
|1| 202241 | 20.06 | 31.5| 1.0 | 32 | 1.1 |
|4| 726358 | 72.04 | 123.9 | 3.1 | 132 | 3.6 |
|8|1074405 | 106.56| 197 | 5.5 | 205| 5.1|
|16| 1227892 | 121.79 | 229.2 | 8.1 | 233 | 7.3 |

## 5 约束限制

### 5.1 主备同步数据恢复问题

主备同步问题指Mysql使用主从灾备，备库从主库不间断通过binlog恢复数据。由于主备数据同步存在一定的时间差，特别在于某些特定情况，例如网络延迟等问题，导致备库同步恢复的数据与主库有较大差别，导致从备库同步的数据不是一份当前时间的完整镜像。

针对这个问题，我们提供了preSql功能，该功能待补充。

### 5.2 一致性约束

Mysql在数据存储划分中属于RDBMS系统，对外可以提供强一致性数据查询接口。例如当一次同步任务启动运行过程中，当该库存在其他数据写入方写入数据时，MysqlReader完全不会获取到写入更新数据，这是由于数据库本身的快照特性决定的。关于数据库快照特性，请参看[MVCC Wikipedia](https://en.wikipedia.org/wiki/Multiversion_concurrency_control)

上述是在MysqlReader单线程模型下数据同步一致性的特性，由于MysqlReader可以根据用户配置信息使用了并发数据抽取，因此不能严格保证数据一致性：当MysqlReader根据splitPk进行数据切分后，会先后启动多个并发任务完成数据同步。由于多个并发任务相互之间不属于同一个读事务，同时多个并发任务存在时间间隔。因此这份数据并不是`完整的`、`一致的`数据快照信息。

针对多线程的一致性快照需求，在技术上目前无法实现，只能从工程角度解决，工程化的方式存在取舍，我们提供几个解决思路给用户，用户可以自行选择：

1. 使用单线程同步，即不再进行数据切片。缺点是速度比较慢，但是能够很好保证一致性。

2. 关闭其他数据写入方，保证当前数据为静态数据，例如，锁表、关闭备库同步等等。缺点是可能影响在线业务。

### 5.3 数据库编码问题

Mysql本身的编码设置非常灵活，包括指定编码到库、表、字段级别，甚至可以均不同编码。优先级从高到低为字段、表、库、实例。我们不推荐数据库用户设置如此混乱的编码，最好在库级别就统一到UTF-8。

MysqlReader底层使用JDBC进行数据抽取，JDBC天然适配各类编码，并在底层进行了编码转换。因此MysqlReader不需用户指定编码，可以自动获取编码并转码。

对于Mysql底层写入编码和其设定的编码不一致的混乱情况，MysqlReader对此无法识别，对此也无法提供解决方案，对于这类情况，`导出有可能为乱码`。

### 5.4 增量数据同步

MysqlReader使用JDBC SELECT语句完成数据抽取工作，因此可以使用SELECT...WHERE...进行增量数据抽取，方式有多种：

* 数据库在线应用写入数据库时，填充modify字段为更改时间戳，包括新增、更新、删除(逻辑删)。对于这类应用，MysqlReader只需要WHERE条件跟上一同步阶段时间戳即可。
* 对于新增流水型数据，MysqlReader可以WHERE条件后跟上一阶段最大自增ID即可。

对于业务上无字段区分新增、修改数据情况，MysqlReader也无法进行增量数据同步，只能同步全量数据。

### 5.5 Sql安全性

MysqlReader提供querySql语句交给用户自己实现SELECT抽取语句，MysqlReader本身对querySql不做任何安全性校验。这块交由DataX用户方自己保证。

## 6 FAQ

***

**Q: MysqlReader同步报错，报错信息为XXX**

 A: 网络或者权限问题，请使用mysql命令行测试：

    mysql -u<username> -p<password> -h<ip> -D<database> -e "select * from <表名>"

如果上述命令也报错，那可以证实是环境问题，请联系你的DBA。


