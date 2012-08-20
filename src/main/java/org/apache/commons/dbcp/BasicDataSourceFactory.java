package org.apache.commons.dbcp;

import java.util.Properties;

import com.alibaba.druid.pool.DruidDataSourceFactory;

public class BasicDataSourceFactory extends DruidDataSourceFactory {

    public static BasicDataSource createDataSource(Properties properties) throws Exception {
        BasicDataSource dataSource = new BasicDataSource();
        config(dataSource, properties);
        return dataSource;
    }
}
