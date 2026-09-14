package com.example.callprocessor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.retry.annotation.EnableRetry;

@SpringBootApplication
@EnableRetry
public class CallProcessorApplication {

    public static void main(String[] args) {
        SpringApplication.run(CallProcessorApplication.class, args);
    }
}
