package org.logicalcobwebs.proxool;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.logging.Logger;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.StringRefAddr;
import javax.naming.spi.ObjectFactory;
import javax.sql.DataSource;

import com.alibaba.druid.pool.DruidDataSource;

public class ProxoolDataSource implements DataSource, ObjectFactory {

    private DruidDataSource druid              = new DruidDataSource();

    private Properties      delegateProperties = new Properties();

    public ProxoolDataSource(){

    }

    public ProxoolDataSource(String alias){
        druid.setName(alias);
    }

    public PrintWriter getLogWriter() throws SQLException {
        return druid.getLogWriter();
    }

    public void setLogWriter(PrintWriter out) throws SQLException {
        druid.setLogWriter(out);
    }

    public void setLoginTimeout(int seconds) throws SQLException {
        druid.setLoginTimeout(seconds);
    }

    public int getLoginTimeout() throws SQLException {
        return druid.getLoginTimeout();
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return druid.getParentLogger();
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        return druid.unwrap(iface);
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return druid.isWrapperFor(iface);
    }

    @SuppressWarnings("rawtypes")
    public Object getObjectInstance(Object refObject, Name name, Context context, Hashtable hashtable) throws Exception {
        if (!(refObject instanceof Reference)) {
            return null;
        }
        Reference reference = (Reference) refObject;
        populatePropertiesFromReference(reference);
        return this;
    }

    public Connection getConnection() throws SQLException {
        return druid.getConnection();
    }

    public Connection getConnection(String username, String password) throws SQLException {
        return druid.getConnection(username, password);
    }

    public String getAlias() {
        return druid.getName();
    }

    public void setAlias(String alias) {
        druid.setName(alias);
    }

    public String getDriverUrl() {
        return druid.getUrl();
    }

    public void setDriverUrl(String url) {
        druid.setUrl(url);
    }

    /**
     * @see ConnectionPoolDefinitionIF#getDriver
     */
    public String getDriver() {
        return druid.getDriverClassName();
    }

    /**
     * @see ConnectionPoolDefinitionIF#getDriver
     */
    public void setDriver(String driver) {
        druid.setDriverClassName(driver);
    }

    /**
     * @see ConnectionPoolDefinitionIF#getMaximumConnectionLifetime
     */
    public long getMaximumConnectionLifetime() {
        return Long.MAX_VALUE;
    }

    /**
     * @see ConnectionPoolDefinitionIF#getMaximumConnectionLifetime
     */
    @Deprecated
    public void setMaximumConnectionLifetime(int maximumConnectionLifetime) {

    }

    /**
     * @see ConnectionPoolDefinitionIF#getPrototypeCount
     */
    @Deprecated
    public int getPrototypeCount() {
        return 0;
    }

    /**
     * @see ConnectionPoolDefinitionIF#getPrototypeCount
     */
    @Deprecated
    public void setPrototypeCount(int prototypeCount) {

    }

    /**
     * @see ConnectionPoolDefinitionIF#getMinimumConnectionCount
     */
    public int getMinimumConnectionCount() {
        return druid.getMinIdle();
    }

    /**
     * @see ConnectionPoolDefinitionIF#getMinimumConnectionCount
     */
    public void setMinimumConnectionCount(int minimumConnectionCount) {
        druid.setMinIdle(minimumConnectionCount);
    }

    /**
     * @see ConnectionPoolDefinitionIF#getMaximumConnectionCount
     */
    public int getMaximumConnectionCount() {
        return druid.getMaxActive();
    }

    /**
     * @see ConnectionPoolDefinitionIF#getMaximumConnectionCount
     */
    public void setMaximumConnectionCount(int maximumConnectionCount) {
        druid.setMaxActive(maximumConnectionCount);
    }

    /**
     * @see ConnectionPoolDefinitionIF#getHouseKeepingSleepTime
     */
    public long getHouseKeepingSleepTime() {
        return druid.getTimeBetweenEvictionRunsMillis();
    }

    /**
     * @see ConnectionPoolDefinitionIF#getHouseKeepingSleepTime
     */
    public void setHouseKeepingSleepTime(int houseKeepingSleepTime) {
        druid.setTimeBetweenEvictionRunsMillis(houseKeepingSleepTime);
    }

    /**
     * @see ConnectionPoolDefinitionIF#getSimultaneousBuildThrottle
     */
    public int getSimultaneousBuildThrottle() {
        return 0;
    }

    /**
     * @see ConnectionPoolDefinitionIF#getSimultaneousBuildThrottle
     */
    public void setSimultaneousBuildThrottle(int simultaneousBuildThrottle) {

    }

    /**
     * @see ConnectionPoolDefinitionIF#getRecentlyStartedThreshold
     */
    public long getRecentlyStartedThreshold() {
        return 0;
    }

    /**
     * @see ConnectionPoolDefinitionIF#getRecentlyStartedThreshold
     */
    public void setRecentlyStartedThreshold(int recentlyStartedThreshold) {

    }

    /**
     * @see ConnectionPoolDefinitionIF#getOverloadWithoutRefusalLifetime
     */
    public long getOverloadWithoutRefusalLifetime() {
        return 0;
    }

    /**
     * @see ConnectionPoolDefinitionIF#getOverloadWithoutRefusalLifetime
     */
    public void setOverloadWithoutRefusalLifetime(int overloadWithoutRefusalLifetime) {

    }

    /**
     * @see ConnectionPoolDefinitionIF#getMaximumActiveTime
     */
    public long getMaximumActiveTime() {
        return druid.getRemoveAbandonedTimeoutMillis();
    }

    /**
     * @see ConnectionPoolDefinitionIF#getMaximumActiveTime
     */
    public void setMaximumActiveTime(long maximumActiveTime) {
        druid.setRemoveAbandonedTimeoutMillis(maximumActiveTime);
    }

    /**
     * @see ConnectionPoolDefinitionIF#isVerbose
     */
    public boolean isVerbose() {
        return false;
    }

    /**
     * @see ConnectionPoolDefinitionIF#isVerbose
     */
    @Deprecated
    public void setVerbose(boolean verbose) {

    }

    /**
     * @see ConnectionPoolDefinitionIF#isTrace
     */
    public boolean isTrace() {
        return false;
    }

    /**
     * @see ConnectionPoolDefinitionIF#isTrace
     */
    @Deprecated
    public void setTrace(boolean trace) {
    }

    /**
     * @see ConnectionPoolDefinitionIF#getStatistics
     */
    public String getStatistics() {
        return "";
    }

    /**
     * @see ConnectionPoolDefinitionIF#getStatistics
     */
    public void setStatistics(String statistics) {
    }

    /**
     * @see ConnectionPoolDefinitionIF#getStatisticsLogLevel
     */
    public String getStatisticsLogLevel() {
        return "";
    }

    /**
     * @see ConnectionPoolDefinitionIF#getStatisticsLogLevel
     */
    public void setStatisticsLogLevel(String statisticsLogLevel) {
    }

    /**
     * @see ConnectionPoolDefinitionIF#getFatalSqlExceptions
     */
    public String getFatalSqlExceptionsAsString() {
        return null;
    }

    /**
     * @see ConnectionPoolDefinitionIF#getFatalSqlExceptions
     */
    @Deprecated
    public void setFatalSqlExceptionsAsString(String fatalSqlExceptionsAsString) {

    }

    /**
     * @see ConnectionPoolDefinitionIF#getFatalSqlExceptionWrapper()
     */
    public String getFatalSqlExceptionWrapperClass() {
        return null;
    }

    /**
     * @see ConnectionPoolDefinitionIF#getFatalSqlExceptionWrapper()
     */
    public void setFatalSqlExceptionWrapperClass(String fatalSqlExceptionWrapperClass) {

    }

    /**
     * @see ConnectionPoolDefinitionIF#getHouseKeepingTestSql
     */
    public String getHouseKeepingTestSql() {
        return druid.getValidationQuery();
    }

    /**
     * @see ConnectionPoolDefinitionIF#getHouseKeepingTestSql
     */
    public void setHouseKeepingTestSql(String houseKeepingTestSql) {
        druid.setValidationQuery(houseKeepingTestSql);
    }

    /**
     * @see ConnectionPoolDefinitionIF#getUser
     */
    public String getUser() {
        return druid.getUsername();
    }

    /**
     * @see ConnectionPoolDefinitionIF#getUser
     */
    public void setUser(String user) {
        druid.setUsername(user);
    }

    /**
     * @see ConnectionPoolDefinitionIF#getPassword
     */
    public String getPassword() {
        return druid.getPassword();
    }

    /**
     * @see ConnectionPoolDefinitionIF#getPassword
     */
    public void setPassword(String password) {
        druid.setPassword(password);
    }

    /**
     * @see ConnectionPoolDefinitionIF#isJmx()
     */
    public boolean isJmx() {
        return true;
    }

    /**
     * @see ConnectionPoolDefinitionIF#isJmx()
     */
    public void setJmx(boolean jmx) {

    }

    /**
     * @see ConnectionPoolDefinitionIF#getJmxAgentId()
     */
    @Deprecated
    public String getJmxAgentId() {
        return "";
    }

    /**
     * @see ConnectionPoolDefinitionIF#getJmxAgentId()
     */
    @Deprecated
    public void setJmxAgentId(String jmxAgentId) {

    }

    /**
     * @see ConnectionPoolDefinitionIF#isTestBeforeUse
     */
    public boolean isTestBeforeUse() {
        return druid.isTestOnBorrow();
    }

    /**
     * @see ConnectionPoolDefinitionIF#isTestBeforeUse
     */
    public void setTestBeforeUse(boolean testBeforeUse) {
        druid.setTestOnBorrow(testBeforeUse);
    }

    /**
     * @see ConnectionPoolDefinitionIF#isTestAfterUse
     */
    public boolean isTestAfterUse() {
        return druid.isTestOnReturn();
    }

    /**
     * @see ConnectionPoolDefinitionIF#isTestAfterUse
     */
    public void setTestAfterUse(boolean testAfterUse) {
        druid.setTestOnReturn(testAfterUse);
    }

    private void populatePropertiesFromReference(Reference reference) {
        RefAddr property = reference.get(ProxoolConstants.ALIAS_PROPERTY);
        if (property != null) {
            setAlias(property.getContent().toString());
        }
        property = reference.get(ProxoolConstants.DRIVER_CLASS_PROPERTY);
        if (property != null) {
            setDriver(property.getContent().toString());
        }
        property = reference.get(ProxoolConstants.FATAL_SQL_EXCEPTION_WRAPPER_CLASS_PROPERTY);
        if (property != null) {
            setFatalSqlExceptionWrapperClass(property.getContent().toString());
        }
        property = reference.get(ProxoolConstants.HOUSE_KEEPING_SLEEP_TIME_PROPERTY);
        if (property != null) {
            setHouseKeepingSleepTime(Integer.valueOf(property.getContent().toString()).intValue());
        }
        property = reference.get(ProxoolConstants.HOUSE_KEEPING_TEST_SQL_PROPERTY);
        if (property != null) {
            setHouseKeepingTestSql(property.getContent().toString());
        }
        property = reference.get(ProxoolConstants.MAXIMUM_CONNECTION_COUNT_PROPERTY);
        if (property != null) {
            setMaximumConnectionCount(Integer.valueOf(property.getContent().toString()).intValue());
        }
        property = reference.get(ProxoolConstants.MAXIMUM_CONNECTION_LIFETIME_PROPERTY);
        if (property != null) {
            setMaximumConnectionLifetime(Integer.valueOf(property.getContent().toString()).intValue());
        }
        property = reference.get(ProxoolConstants.MAXIMUM_ACTIVE_TIME_PROPERTY);
        if (property != null) {
            setMaximumActiveTime(Long.valueOf(property.getContent().toString()).intValue());
        }
        property = reference.get(ProxoolConstants.MINIMUM_CONNECTION_COUNT_PROPERTY);
        if (property != null) {
            setMinimumConnectionCount(Integer.valueOf(property.getContent().toString()).intValue());
        }
        property = reference.get(ProxoolConstants.OVERLOAD_WITHOUT_REFUSAL_LIFETIME_PROPERTY);
        if (property != null) {
            setOverloadWithoutRefusalLifetime(Integer.valueOf(property.getContent().toString()).intValue());
        }
        property = reference.get(ProxoolConstants.PASSWORD_PROPERTY);
        if (property != null) {
            setPassword(property.getContent().toString());
        }
        property = reference.get(ProxoolConstants.PROTOTYPE_COUNT_PROPERTY);
        if (property != null) {
            setPrototypeCount(Integer.valueOf(property.getContent().toString()).intValue());
        }
        property = reference.get(ProxoolConstants.RECENTLY_STARTED_THRESHOLD_PROPERTY);
        if (property != null) {
            setRecentlyStartedThreshold(Integer.valueOf(property.getContent().toString()).intValue());
        }
        property = reference.get(ProxoolConstants.SIMULTANEOUS_BUILD_THROTTLE_PROPERTY);
        if (property != null) {
            setSimultaneousBuildThrottle(Integer.valueOf(property.getContent().toString()).intValue());
        }
        property = reference.get(ProxoolConstants.STATISTICS_PROPERTY);
        if (property != null) {
            setStatistics(property.getContent().toString());
        }
        property = reference.get(ProxoolConstants.STATISTICS_LOG_LEVEL_PROPERTY);
        if (property != null) {
            setStatisticsLogLevel(property.getContent().toString());
        }
        property = reference.get(ProxoolConstants.TRACE_PROPERTY);
        if (property != null) {
            setTrace("true".equalsIgnoreCase(property.getContent().toString()));
        }
        property = reference.get(ProxoolConstants.DRIVER_URL_PROPERTY);
        if (property != null) {
            setDriverUrl(property.getContent().toString());
        }
        property = reference.get(ProxoolConstants.USER_PROPERTY);
        if (property != null) {
            setUser(property.getContent().toString());
        }
        property = reference.get(ProxoolConstants.VERBOSE_PROPERTY);
        if (property != null) {
            setVerbose("true".equalsIgnoreCase(property.getContent().toString()));
        }
        property = reference.get(ProxoolConstants.JMX_PROPERTY);
        if (property != null) {
            setJmx("true".equalsIgnoreCase(property.getContent().toString()));
        }
        property = reference.get(ProxoolConstants.JMX_AGENT_PROPERTY);
        if (property != null) {
            setJmxAgentId(property.getContent().toString());
        }
        property = reference.get(ProxoolConstants.TEST_BEFORE_USE_PROPERTY);
        if (property != null) {
            setTestBeforeUse("true".equalsIgnoreCase(property.getContent().toString()));
        }
        property = reference.get(ProxoolConstants.TEST_AFTER_USE_PROPERTY);
        if (property != null) {
            setTestAfterUse("true".equalsIgnoreCase(property.getContent().toString()));
        }
        // Pick up any properties that we don't recognise
        Enumeration<?> e = reference.getAll();
        while (e.hasMoreElements()) {
            StringRefAddr stringRefAddr = (StringRefAddr) e.nextElement();
            String name = stringRefAddr.getType();
            String content = stringRefAddr.getContent().toString();
            if (name.indexOf(ProxoolConstants.PROPERTY_PREFIX) != 0) {
                delegateProperties.put(name, content);
            }
        }
    }

    public void setDelegateProperties(String properties) {
        StringTokenizer stOuter = new StringTokenizer(properties, ",");
        while (stOuter.hasMoreTokens()) {
            StringTokenizer stInner = new StringTokenizer(stOuter.nextToken(), "=");
            if (stInner.countTokens() == 1) {
                // Allow blank string to be a valid value
                delegateProperties.put(stInner.nextToken().trim(), "");
            } else if (stInner.countTokens() == 2) {
                delegateProperties.put(stInner.nextToken().trim(), stInner.nextToken().trim());
            } else {
                throw new IllegalArgumentException("Unexpected delegateProperties value: '" + properties
                                                   + "'. Expected 'name=value'");
            }
        }
    }
}
