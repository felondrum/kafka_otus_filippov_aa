package com.example.transcription.analyzer.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.init.DataSourceInitializer;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.core.io.ClassPathResource;

import javax.sql.DataSource;

@Configuration
public class DataSourceConfig {
    // PostgreSQL datasource is configured via application.yml
    // This class can be extended if we need programmatic initialization
}
