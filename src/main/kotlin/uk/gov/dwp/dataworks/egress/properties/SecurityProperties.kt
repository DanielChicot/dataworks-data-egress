package uk.gov.dwp.dataworks.egress.properties

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
@ConfigurationProperties(prefix = "security")
class SecurityProperties(var identityStore: String = "",
                         var identityStorePassword: String = "",
                         var identityStoreAlias: String = "",
                         var identityKeyPassword: String = "",
                         var trustStore: String = "",
                         var trustStorePassword: String = "",
                         var connectTimeout: Int = 300_000,
                         var connectionRequestTimeout: Int = 300_000,
                         var socketTimeout: Int = 300_000) {


    @Bean
    fun identityStore() = identityStore

    @Bean
    fun identityStorePassword(): String = identityStorePassword

    @Bean
    fun identityStoreAlias(): String = identityStoreAlias

    @Bean
    fun identityKeyPassword(): String = identityKeyPassword

    @Bean
    fun trustStore(): String = trustStore

    @Bean
    fun trustStorePassword(): String = trustStorePassword

    @Bean
    fun connectTimeout(): Int = connectTimeout

    @Bean
    fun connectionRequestTimeout(): Int = connectionRequestTimeout

    @Bean
    fun socketTimeout(): Int = socketTimeout
}
