package org.apache.commons.dbcp;

import java.util.Properties;

import javax.sql.DataSource;

public class ManagedBasicDataSourceFactory extends BasicDataSourceFactory {

    protected DataSource createDataSourceInternal(Properties properties) throws Exception {
        ManagedBasicDataSource dataSource = new ManagedBasicDataSource();
        config(dataSource, properties);
        return dataSource;
    }

    public static DataSource createDataSource(Properties properties) throws Exception {
        ManagedBasicDataSource dataSource = new ManagedBasicDataSource();
        config(dataSource, properties);
        return dataSource;
    }
}
