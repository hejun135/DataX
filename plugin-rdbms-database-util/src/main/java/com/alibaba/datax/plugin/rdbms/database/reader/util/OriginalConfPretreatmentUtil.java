package com.alibaba.datax.plugin.rdbms.database.reader.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.ListUtil;
import com.alibaba.datax.plugin.rdbms.database.reader.Constant;
import com.alibaba.datax.plugin.rdbms.database.reader.Key;
import com.alibaba.datax.plugin.rdbms.database.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.database.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.database.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.database.util.TableExpandUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public final class OriginalConfPretreatmentUtil {
    private static final Logger LOG = LoggerFactory
            .getLogger(OriginalConfPretreatmentUtil.class);

    public static DataBaseType DATABASE_TYPE;

    public static void doPretreatment(Configuration originalConfig) {
        // 检查 username/password 配置（必填）
        originalConfig.getNecessaryValue(Key.USERNAME,
                DBUtilErrorCode.REQUIRED_VALUE);
        originalConfig.getNecessaryValue(Key.PASSWORD,
                DBUtilErrorCode.REQUIRED_VALUE);
        //dealWhere(originalConfig);

        simplifyConf(originalConfig);
    }

    public static void dealWhere(Configuration originalConfig) {
        String where = originalConfig.getString(Key.WHERE, null);
        if(StringUtils.isNotBlank(where)) {
            String whereImprove = where.trim();
            if(whereImprove.endsWith(";") || whereImprove.endsWith("；")) {
                whereImprove = whereImprove.substring(0,whereImprove.length()-1);
            }
            originalConfig.set(Key.WHERE, whereImprove);
        }
    }

    /**
     * 对配置进行初步处理：
     * <ol>
     * <li>处理同一个数据库配置了多个jdbcUrl的情况</li>
     * <li>识别并标记是采用querySql 模式还是 table 模式</li>
     * <li>对 table 模式，确定分表个数，并处理 column 转 *事项</li>
     * </ol>
     */
    private static void simplifyConf(Configuration originalConfig) {
        boolean isTableMode = recognizeTableOrQuerySqlMode(originalConfig);
        originalConfig.set(Constant.IS_TABLE_MODE, isTableMode);

        dealJdbcAndTable(originalConfig);
    }

    /**
     * 构建jdbc和表信息
     * @param originalConfig
     */
    private static void dealJdbcAndTable(Configuration originalConfig) {
        String username = originalConfig.getString(Key.USERNAME);
        String password = originalConfig.getString(Key.PASSWORD);
        boolean checkSlave = originalConfig.getBool(Key.CHECK_SLAVE, false);
        boolean isTableMode = originalConfig.getBool(Constant.IS_TABLE_MODE);
        boolean isPreCheck = originalConfig.getBool(Key.DRYRUN,false);

        List<Object> conns = originalConfig.getList(Constant.CONN_MARK,
                Object.class);
        List<String> preSql = originalConfig.getList(Key.PRE_SQL, String.class);

        int tableNum = 0;

        for (int i = 0, len = conns.size(); i < len; i++) {
            Configuration connConf = Configuration
                    .from(conns.get(i).toString());

            connConf.getNecessaryValue(Key.JDBC_URL,
                    DBUtilErrorCode.REQUIRED_VALUE);

            List<String> jdbcUrls = connConf
                    .getList(Key.JDBC_URL, String.class);

            String jdbcUrl;
            if (isPreCheck) {
                jdbcUrl = DBUtil.chooseJdbcUrlWithoutRetry(DATABASE_TYPE, jdbcUrls,
                        username, password, preSql, checkSlave);
            } else {
                jdbcUrl = DBUtil.chooseJdbcUrl(DATABASE_TYPE, jdbcUrls,
                        username, password, preSql, checkSlave);
            }

            jdbcUrl = DATABASE_TYPE.appendJDBCSuffixForReader(jdbcUrl);

            // 回写到connection[i].jdbcUrl
            originalConfig.set(String.format("%s[%d].%s", Constant.CONN_MARK,
                    i, Key.JDBC_URL), jdbcUrl);

            LOG.info("Available jdbcUrl:{}.",jdbcUrl);
            List<String> tables = null;
            if (isTableMode) {
                // table 方式
                // 对每一个connection 上配置的table 项进行解析(已对表名称进行了 ` 处理的)
                tables = connConf.getList(Key.TABLE, String.class);
            } else {
                // 说明是配置的database模式，则需要通过database获取其下的所有基础表，设置回config中
                String database = connConf.getString(Key.DATABASE);
                tables = DBUtil.getTables(DATABASE_TYPE,jdbcUrl,username,password,database);
                if (tables == null || tables.isEmpty()){
                    throw DataXException.asDataXException(
                            DBUtilErrorCode.ILLEGAL_VALUE, String.format("您所配置的读取数据库:%s 不正确. 因为DataX根据您的配置找不到该库下的表. 请检查您的配置并作出修改." +
                                    "请先了解 DataX 配置.", database));
                }
                originalConfig.set(Constant.IS_TABLE_MODE, true);
            }
            List<String> expandedTables = TableExpandUtil.expandTableConf(
                    DATABASE_TYPE, tables);

            if (null == expandedTables || expandedTables.isEmpty()) {
                throw DataXException.asDataXException(
                        DBUtilErrorCode.ILLEGAL_VALUE, String.format("您所配置的读取数据库表:%s 不正确. 因为DataX根据您的配置找不到这张表. 请检查您的配置并作出修改." +
                                "请先了解 DataX 配置.", StringUtils.join(tables, ",")));
            }

            tableNum += expandedTables.size();

            originalConfig.set(String.format("%s[%d].%s",
                    Constant.CONN_MARK, i, Key.TABLE), expandedTables);

            dealColumnConf(originalConfig,i,expandedTables);
        }

        originalConfig.set(Constant.TABLE_NUMBER_MARK, tableNum);
    }

    /**
     * 构建列信息
     * @param originalConfig
     * @param i
     * @param tables
     */
    private static void dealColumnConf(Configuration originalConfig,Integer i,List<String> tables) {
        String jdbcUrl = originalConfig.getString(String.format(
                "%s[%d].%s", Constant.CONN_MARK,i, Key.JDBC_URL));
        String username = originalConfig.getString(Key.USERNAME);
        String password = originalConfig.getString(Key.PASSWORD);
        for (int j = 0; j < tables.size(); j++) {
            String tableName = tables.get(j);
            List<String> allColumns = DBUtil.getTableColumns(
                    DATABASE_TYPE, jdbcUrl, username, password,
                    tableName);
            LOG.info("table:[{}] has columns:[{}].",
                    tableName, StringUtils.join(allColumns, ","));
            // warn:注意mysql表名区分大小写
            allColumns = ListUtil.valueToLowerCase(allColumns);
            originalConfig.set(String.format("%s[%d].%s[%d]", Constant.CONN_MARK, i,Key.COLUMN_LIST,j), allColumns);
            originalConfig.set(String.format("%s[%d].%s[%d]", Constant.CONN_MARK, i,Key.COLUMN,j),
                    StringUtils.join(allColumns, ","));
        }
    }

    private static boolean recognizeTableOrQuerySqlMode(
            Configuration originalConfig) {
        List<Object> conns = originalConfig.getList(Constant.CONN_MARK,
                Object.class);

        List<Boolean> tableModeFlags = new ArrayList<Boolean>();
        List<Boolean> querySqlModeFlags = new ArrayList<Boolean>();

        String table = null;
        String database = null;

        boolean isTableMode = false;
        boolean isQuerySqlMode = false;
        for (int i = 0, len = conns.size(); i < len; i++) {
            Configuration connConf = Configuration
                    .from(conns.get(i).toString());
            table = connConf.getString(Key.TABLE, null);
            database = connConf.getString(Key.DATABASE, null);

            isTableMode = StringUtils.isNotBlank(table);
            tableModeFlags.add(isTableMode);

            isQuerySqlMode = StringUtils.isNotBlank(database);
            querySqlModeFlags.add(isQuerySqlMode);

            if (false == isTableMode && false == isQuerySqlMode) {
                // table 和 database 二者均未配制
                throw DataXException.asDataXException(
                        DBUtilErrorCode.TABLE_QUERYSQL_MISSING, "您的配置有误. 因为table和database应该配置并且只能配置一个. 请检查您的配置并作出修改.");
            } else if (true == isTableMode && true == isQuerySqlMode) {
                // table 和 database 二者均配置
                throw DataXException.asDataXException(DBUtilErrorCode.TABLE_QUERYSQL_MIXED,
                        "您的配置凌乱了. 因为datax不能同时既配置table又配置database.请检查您的配置并作出修改.");
            }
        }

        // 混合配制 table 和 database
        if (!ListUtil.checkIfValueSame(tableModeFlags)
                || !ListUtil.checkIfValueSame(tableModeFlags)) {
            throw DataXException.asDataXException(DBUtilErrorCode.TABLE_QUERYSQL_MIXED,
                    "您配置凌乱了. 不能同时既配置table又配置database. 请检查您的配置并作出修改.");
        }

        return tableModeFlags.get(0);
    }

}
