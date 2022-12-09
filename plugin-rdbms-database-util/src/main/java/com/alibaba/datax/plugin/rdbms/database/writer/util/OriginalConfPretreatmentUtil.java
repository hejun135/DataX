package com.alibaba.datax.plugin.rdbms.database.writer.util;

import com.alibaba.datax.common.constant.CommonConstant;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.ListUtil;
import com.alibaba.datax.plugin.rdbms.database.util.*;
import com.alibaba.datax.plugin.rdbms.util.*;
import com.alibaba.datax.plugin.rdbms.database.writer.Constant;
import com.alibaba.datax.plugin.rdbms.database.writer.Key;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public final class OriginalConfPretreatmentUtil {
    private static final Logger LOG = LoggerFactory
            .getLogger(OriginalConfPretreatmentUtil.class);

    public static DataBaseType DATABASE_TYPE;

//    public static void doPretreatment(Configuration originalConfig) {
//        doPretreatment(originalConfig,null);
//    }

    public static void doPretreatment(Configuration originalConfig, DataBaseType dataBaseType) {
        // 检查 username/password 配置（必填）
        originalConfig.getNecessaryValue(Key.USERNAME, DBUtilErrorCode.REQUIRED_VALUE);
        originalConfig.getNecessaryValue(Key.PASSWORD, DBUtilErrorCode.REQUIRED_VALUE);

        doCheckBatchSize(originalConfig);

        simplifyConf(originalConfig);

        //dealColumnConf(originalConfig);
        dealWriteMode(originalConfig, dataBaseType);
    }

    public static void doCheckBatchSize(Configuration originalConfig) {
        // 检查batchSize 配置（选填，如果未填写，则设置为默认值）
        int batchSize = originalConfig.getInt(Key.BATCH_SIZE, Constant.DEFAULT_BATCH_SIZE);
        if (batchSize < 1) {
            throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE, String.format(
                    "您的batchSize配置有误. 您所配置的写入数据库表的 batchSize:%s 不能小于1. 推荐配置范围为：[100-1000], 该值越大, 内存溢出可能性越大. 请检查您的配置并作出修改.",
                    batchSize));
        }

        originalConfig.set(Key.BATCH_SIZE, batchSize);
    }

    public static void simplifyConf(Configuration originalConfig) {
        List<Object> connections = originalConfig.getList(Constant.CONN_MARK,
                Object.class);

        int tableNum = 0;
        String username = originalConfig.getString(com.alibaba.datax.plugin.rdbms.database.reader.Key.USERNAME);
        String password = originalConfig.getString(com.alibaba.datax.plugin.rdbms.database.reader.Key.PASSWORD);
        for (int i = 0, len = connections.size(); i < len; i++) {
            Configuration connConf = Configuration.from(connections.get(i).toString());

            String jdbcUrl = connConf.getString(Key.JDBC_URL);
            if (StringUtils.isBlank(jdbcUrl)) {
                throw DataXException.asDataXException(DBUtilErrorCode.REQUIRED_VALUE, "您未配置的写入数据库表的 jdbcUrl.");
            }

            jdbcUrl = DATABASE_TYPE.appendJDBCSuffixForWriter(jdbcUrl);
            originalConfig.set(String.format("%s[%d].%s", Constant.CONN_MARK, i, Key.JDBC_URL),
                    jdbcUrl);

            List<String> tables = connConf.getList(Key.TABLE, String.class);
            if (null == tables || tables.isEmpty()) {
                // 说明是配置的database模式，则需要通过database获取其下的所有基础表，设置回config中
                String database = connConf.getString(com.alibaba.datax.plugin.rdbms.database.reader.Key.DATABASE);
                if (StringUtils.isBlank(database)){
                    throw DataXException.asDataXException(DBUtilErrorCode.REQUIRED_VALUE,
                            "您未配置写入数据库表的表名称和数据库名称. 根据配置DataX找不到您配置的表. 请检查您的配置并作出修改.");
                }
                tables = DBUtil.getTables(DATABASE_TYPE,jdbcUrl,username,password,database);
                originalConfig.set(Constant.IS_TABLE_MODE, false);
            }else {
                originalConfig.set(Constant.IS_TABLE_MODE, true);
            }
            if (null == tables || tables.isEmpty()) {
                throw DataXException.asDataXException(DBUtilErrorCode.REQUIRED_VALUE,
                        "您未配置写入数据库表的表名称. 根据配置DataX找不到您配置的表. 请检查您的配置并作出修改.");
            }

            // 对每一个connection 上配置的table 项进行解析
            List<String> expandedTables = TableExpandUtil
                    .expandTableConf(DATABASE_TYPE, tables);

            if (null == expandedTables || expandedTables.isEmpty()) {
                throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR,
                        "您配置的写入数据库表名称错误. DataX找不到您配置的表，请检查您的配置并作出修改.");
            }

            tableNum += expandedTables.size();

            originalConfig.set(String.format("%s[%d].%s", Constant.CONN_MARK,
                    i, Key.TABLE), expandedTables);
            dealColumnConf(originalConfig,i,expandedTables);
        }

        originalConfig.set(Constant.TABLE_NUMBER_MARK, tableNum);
    }

    public static void dealColumnConf(Configuration originalConfig, ConnectionFactory connectionFactory,Integer i,Integer j, String oneTable) {

            boolean isPreCheck = originalConfig.getBool(Key.DRYRUN, false);
            List<String> allColumns;
            if (isPreCheck){
                allColumns = DBUtil.getTableColumnsByConn(DATABASE_TYPE,connectionFactory.getConnecttionWithoutRetry(), oneTable, connectionFactory.getConnectionInfo());
            }else{
                allColumns = DBUtil.getTableColumnsByConn(DATABASE_TYPE,connectionFactory.getConnecttion(), oneTable, connectionFactory.getConnectionInfo());
            }

            LOG.info("table:[{}] all columns:[\n{}\n].", oneTable,
                    StringUtils.join(allColumns, ","));
        // 回填其值，需要以 String 的方式转交后续处理
        originalConfig.set(String.format("%s[%d].%s[%d]", Constant.CONN_MARK, i, Key.COLUMN,j), allColumns);
    }

    public static void dealColumnConf(Configuration originalConfig,Integer i,List<String> tables) {
        String jdbcUrl = originalConfig.getString(String.format("%s[0].%s",
                Constant.CONN_MARK, Key.JDBC_URL));

        String username = originalConfig.getString(Key.USERNAME);
        String password = originalConfig.getString(Key.PASSWORD);

        JdbcConnectionFactory jdbcConnectionFactory = new JdbcConnectionFactory(DATABASE_TYPE, jdbcUrl, username, password);
        for (int j = 0; j < tables.size(); j++) {
            String tableName = tables.get(j);
            dealColumnConf(originalConfig, jdbcConnectionFactory,i,j, tableName);
        }
    }

    public static void dealWriteMode(Configuration originalConfig, DataBaseType dataBaseType) {
        List<String> columns = originalConfig.getList(Key.COLUMN, String.class);

        String jdbcUrl = originalConfig.getString(String.format("%s[0].%s",
                Constant.CONN_MARK, Key.JDBC_URL, String.class));

        // 默认为：insert 方式
        String writeMode = originalConfig.getString(Key.WRITE_MODE, "INSERT");

        List<String> valueHolders = new ArrayList<String>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            valueHolders.add("?");
        }

        boolean forceUseUpdate = false;
        //ob10的处理
        if (dataBaseType == DataBaseType.MySql && isOB10(jdbcUrl)) {
            forceUseUpdate = true;
        }

        String writeDataSqlTemplate = WriterUtil.getWriteTemplate(columns, valueHolders, writeMode,dataBaseType, forceUseUpdate);

        LOG.info("Write data [\n{}\n], which jdbcUrl like:[{}]", writeDataSqlTemplate, jdbcUrl);

        originalConfig.set(Constant.INSERT_OR_REPLACE_TEMPLATE_MARK, writeDataSqlTemplate);
    }

    public static boolean isOB10(String jdbcUrl) {
        //ob10的处理
        if (jdbcUrl.startsWith(Constant.OB10_SPLIT_STRING)) {
            String[] ss = jdbcUrl.split(Constant.OB10_SPLIT_STRING_PATTERN);
            if (ss.length != 3) {
                throw DataXException
                        .asDataXException(
                                DBUtilErrorCode.JDBC_OB10_ADDRESS_ERROR, "JDBC OB10格式错误，请联系askdatax");
            }
            return true;
        }
        return false;
    }

}
