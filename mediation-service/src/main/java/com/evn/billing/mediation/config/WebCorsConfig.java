package com.evn.billing.mediation.config;

import org.springframework.context.annotation.Configuration;

/**
 * CORS is handled at the Nginx Ingress Controller level (k8s/05-ingress.yaml).
 * Do NOT add CorsFilter or WebMvcConfigurer CORS here to avoid duplicate headers.
 */
@Configuration
public class WebCorsConfig {
}
