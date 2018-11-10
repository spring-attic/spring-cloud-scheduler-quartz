package org.springframework.cloud.scheduler.spi.quartz;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import org.springframework.boot.jdbc.DatabaseDriver;
import org.springframework.core.io.ResourceLoader;
import org.springframework.jdbc.datasource.init.DatabasePopulatorUtils;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.jdbc.support.MetaDataAccessException;
import org.springframework.util.Assert;

/**
 * Initialize the Quartz Scheduler schema.
 *
 * @author Manokethan Parameswaran
 */
public class QuartzDataSourceInitializer {

    private static final String PLATFORM_PLACEHOLDER = "@@platform@@";

    private final DataSource dataSource;
    private final ResourceLoader resourceLoader;
    private final QuartzSchedulerProperties properties;

    public QuartzDataSourceInitializer(DataSource dataSource,
                                       ResourceLoader resourceLoader, QuartzSchedulerProperties properties) {
        Assert.notNull(dataSource, "DataSource must not be null");
        Assert.notNull(resourceLoader, "ResourceLoader must not be null");
        Assert.notNull(properties, "QuartzProperties must not be null");
        this.dataSource = dataSource;
        this.resourceLoader = resourceLoader;
        this.properties = properties;
    }

    @PostConstruct
    private void initialize() {
        ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
        String schemaLocation = this.properties.getJdbc().getSchema();
        if (schemaLocation.contains(PLATFORM_PLACEHOLDER)) {
            String platform = getDatabaseName();
            schemaLocation = schemaLocation.replace(PLATFORM_PLACEHOLDER, platform);
        }
        populator.addScript(this.resourceLoader.getResource(schemaLocation));
        populator.setContinueOnError(true);
        populator.setCommentPrefix(this.properties.getJdbc().getCommentPrefix());
        DatabasePopulatorUtils.execute(populator, this.dataSource);
    }

    private String getDatabaseName() {
        try {
            String productName = JdbcUtils.commonDatabaseName(JdbcUtils
                    .extractDatabaseMetaData(this.dataSource, "getDatabaseProductName")
                    .toString());
            DatabaseDriver databaseDriver = DatabaseDriver.fromProductName(productName);
            if (databaseDriver == DatabaseDriver.UNKNOWN) {
                throw new IllegalStateException("Unable to detect database type");
            }
            String databaseName = databaseDriver.getId();
            if ("db2".equals(databaseName)) {
                return "db2_v95";
            }
            if ("mysql".equals(databaseName)) {
                return "mysql_innodb";
            }
            if ("postgresql".equals(databaseName)) {
                return "postgres";
            }
            if ("sqlserver".equals(databaseName)) {
                return "sqlServer";
            }
            return databaseName;
        }
        catch (MetaDataAccessException ex) {
            throw new IllegalStateException("Unable to detect database type", ex);
        }
    }

}