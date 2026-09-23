package com.example.transcription.analyzer;

import com.example.transcription.analyzer.config.TestContainerConfig;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@SpringBootTest(classes = {Application.class, TestContainerConfig.class})
@ContextConfiguration(classes = {TestContainerConfig.class})
@Testcontainers
public @interface IntegrationTest {
}
