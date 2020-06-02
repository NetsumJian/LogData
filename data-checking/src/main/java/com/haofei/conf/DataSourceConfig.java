package com.haofei.conf;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

@Configuration
public class DataSourceConfig {

    @Bean
    @Primary
    @ConfigurationProperties("spring.datasource")
    public DataSourceProperties primaryDataSourceProperties() {
        return new DataSourceProperties();
    }

    @Bean("sxgfDataSourceProperties")
    @ConfigurationProperties("spring.sxgf-datasource")
    public DataSourceProperties secondDataSourceProperties() {
        return new DataSourceProperties();
    }

    /**
     * Create primary (default) DataSource.
     */
    @Bean
    @Primary
    public DataSource primaryDataSource(@Autowired DataSourceProperties props) {
        return props.initializeDataSourceBuilder().build();
    }

    /**
     * Create second DataSource and named "secondDatasource".
     */
    @Bean("sxgfDatasource")
    public DataSource secondDataSource(@Autowired @Qualifier("sxgfDataSourceProperties") DataSourceProperties props) {
        return props.initializeDataSourceBuilder().build();
    }

    /**
     * Create primary (default) JdbcTemplate from primary DataSource.
     */
    @Bean
    @Primary
    public JdbcTemplate primaryJdbcTemplate(DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    /**
     * Create second JdbcTemplate from second DataSource.
     */
    @Bean(name = "sxgfJdbcTemplate")
    public JdbcTemplate secondJdbcTemplate(@Autowired @Qualifier("sxgfDatasource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }
}
