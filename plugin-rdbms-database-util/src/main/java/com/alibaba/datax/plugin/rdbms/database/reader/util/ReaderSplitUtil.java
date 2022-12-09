package com.alibaba.datax.plugin.rdbms.database.reader.util;

import com.alibaba.datax.common.constant.CommonConstant;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.database.reader.Constant;
import com.alibaba.datax.plugin.rdbms.database.reader.Key;
import com.alibaba.datax.plugin.rdbms.database.util.DataBaseType;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public final class ReaderSplitUtil {
    private static final Logger LOG = LoggerFactory
            .getLogger(ReaderSplitUtil.class);

    public static List<Configuration> doSplit(
            Configuration originalSliceConfig, int adviceNumber) {
        boolean isTableMode = originalSliceConfig.getBool(Constant.IS_TABLE_MODE).booleanValue();
        int eachTableShouldSplittedNumber = -1;
        if (isTableMode) {
            // adviceNumber这里是channel数量大小, 即datax并发task数量
            // eachTableShouldSplittedNumber是单表应该切分的份数, 向上取整可能和adviceNumber没有比例关系了已经
            eachTableShouldSplittedNumber = calculateEachTableShouldSplittedNumber(
                    adviceNumber, originalSliceConfig.getInt(Constant.TABLE_NUMBER_MARK));
        }


        String where = originalSliceConfig.getString(Key.WHERE, null);

        List<Object> conns = originalSliceConfig.getList(Constant.CONN_MARK, Object.class);

        List<Configuration> splittedConfigs = new ArrayList<Configuration>();

        for (int i = 0, len = conns.size(); i < len; i++) {
            Configuration sliceConfig = originalSliceConfig.clone();

            Configuration connConf = Configuration.from(conns.get(i).toString());
            String jdbcUrl = connConf.getString(Key.JDBC_URL);
            sliceConfig.set(Key.JDBC_URL, jdbcUrl);

            // 抽取 jdbcUrl 中的 ip/port 进行资源使用的打标，以提供给 core 做有意义的 shuffle 操作
            sliceConfig.set(CommonConstant.LOAD_BALANCE_RESOURCE_MARK, DataBaseType.parseIpFromJdbcUrl(jdbcUrl));

            sliceConfig.remove(Constant.CONN_MARK);

            Configuration tempSlice;

            // 说明是配置的 table 方式
            if (isTableMode) {
                // 已在之前进行了扩展和`处理，可以直接使用
                List<String> tables = connConf.getList(Key.TABLE, String.class);

                Validate.isTrue(null != tables && !tables.isEmpty(), "您读取数据库表配置错误.");

                List<String> column = connConf.getList(Key.COLUMN,String.class);
                Validate.isTrue(column.size() != tables.size(), "您读取数据库表和列集合个数不一致.");
                for (int j = 0; j < tables.size(); j++) {
                    String table = tables.get(j);
                    tempSlice = sliceConfig.clone();
                    tempSlice.set(Key.TABLE, table);
                    String queryColumn = HintUtil.buildQueryColumn(jdbcUrl, table, column.get(j));
                    tempSlice.set(Key.QUERY_SQL, SingleTableSplitUtil.buildQuerySql(queryColumn, table, where));
                    splittedConfigs.add(tempSlice);
                }
            }

        }

        return splittedConfigs;
    }

    public static Configuration doPreCheckSplit(Configuration originalSliceConfig) {
        Configuration queryConfig = originalSliceConfig.clone();
        boolean isTableMode = originalSliceConfig.getBool(Constant.IS_TABLE_MODE).booleanValue();

        String splitPK = originalSliceConfig.getString(Key.SPLIT_PK);
        String column = originalSliceConfig.getString(Key.COLUMN);
        String where = originalSliceConfig.getString(Key.WHERE, null);

        List<Object> conns = queryConfig.getList(Constant.CONN_MARK, Object.class);

        for (int i = 0, len = conns.size(); i < len; i++) {
            Configuration connConf = Configuration.from(conns.get(i).toString());
            List<String> querys = new ArrayList<String>();
            List<String> splitPkQuerys = new ArrayList<String>();
            String connPath = String.format("connection[%d]", i);
            // 说明是配置的 table 方式
            if (isTableMode) {
                // 已在之前进行了扩展和`处理，可以直接使用
                List<String> tables = connConf.getList(Key.TABLE, String.class);
                Validate.isTrue(null != tables && !tables.isEmpty(), "您读取数据库表配置错误.");
                for (String table : tables) {
                    querys.add(SingleTableSplitUtil.buildQuerySql(column, table, where));
                    if (splitPK != null && !splitPK.isEmpty()) {
                        splitPkQuerys.add(SingleTableSplitUtil.genPKSql(splitPK.trim(), table, where));
                    }
                }
                if (!splitPkQuerys.isEmpty()) {
                    connConf.set(Key.SPLIT_PK_SQL, splitPkQuerys);
                }
                connConf.set(Key.QUERY_SQL, querys);
                queryConfig.set(connPath, connConf);
            } else {
                // 说明是配置的 querySql 方式
                List<String> sqls = connConf.getList(Key.QUERY_SQL,
                        String.class);
                for (String querySql : sqls) {
                    querys.add(querySql);
                }
                connConf.set(Key.QUERY_SQL, querys);
                queryConfig.set(connPath, connConf);
            }
        }
        return queryConfig;
    }

    private static int calculateEachTableShouldSplittedNumber(int adviceNumber,
                                                              int tableNumber) {
        double tempNum = 1.0 * adviceNumber / tableNumber;

        return (int) Math.ceil(tempNum);
    }

}
