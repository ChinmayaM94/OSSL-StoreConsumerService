package com.safeway.ossl.storeconsumer.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Basic OpenAPI/Swagger metadata. UI is available at /swagger-ui.html and
 * the raw spec at /v3/api-docs once the application is running.
 */
@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI osslStoreConsumerOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("OSSL Store Consumer Service API")
                        .description("Kafka consumers (SQL + MongoDB) and DLQ replay for OSSL store events")
                        .version("1.0.0")
                        .contact(new Contact().name("OSSL Store Consumer Team")));
    }
}
