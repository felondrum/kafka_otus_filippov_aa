package com.example.reportingnps.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI customOpenAPI() {
        final String securitySchemeName = "bearer-key";

        return new OpenAPI()
                .info(new Info()
                        .title("Reporting & NPS Service API")
                        .version("1.0.0")
                        .description("API для получения отчётов по звонкам, аналитики настроений и отчётов по агентам.")
                        .contact(new Contact()
                                .name("Reporting NPS Team")
                                .email("reporting-nps@example.com"))
                        .license(new License()
                                .name("Apache 2.0")
                                .url("https://www.apache.org/licenses/LICENSE-2.0")));
    }
}
