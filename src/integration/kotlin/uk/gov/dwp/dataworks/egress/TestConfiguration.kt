package uk.gov.dwp.dataworks.egress

import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import org.springframework.context.annotation.PropertySource
import uk.gov.dwp.dataworks.egress.configuration.ContextConfiguration
import uk.gov.dwp.dataworks.egress.configuration.LocalstackConfiguration
import uk.gov.dwp.dataworks.egress.properties.AwsProperties
import uk.gov.dwp.dataworks.egress.properties.SecurityProperties
import uk.gov.dwp.dataworks.egress.provider.impl.DksEncryptionMaterialsProvider
import uk.gov.dwp.dataworks.egress.provider.impl.SecureHttpClientProvider
import uk.gov.dwp.dataworks.egress.services.impl.CipherServiceImpl
import uk.gov.dwp.dataworks.egress.services.impl.DataKeyServiceImpl

@Import(LocalstackConfiguration::class,
    ContextConfiguration::class,
    DksEncryptionMaterialsProvider::class,
    DataKeyServiceImpl::class,
    SecureHttpClientProvider::class,
    AwsProperties::class,
    SecurityProperties::class,
    CipherServiceImpl::class)
@Configuration
@PropertySource("classpath:integration.properties")
class TestConfiguration {
    @Bean
    fun identityStore() = "dataworks-data-egress-integration-tests-keystore.jks"

    @Bean
    fun identityStorePassword(): String = "changeit"

    @Bean
    fun identityStoreAlias(): String = "cid"

    @Bean
    fun identityKeyPassword(): String = "changeit"

    @Bean
    fun trustStore(): String = "dataworks-data-egress-integration-tests-truststore.jks"

    @Bean
    fun trustStorePassword(): String = "changeit"

    @Bean
    fun connectTimeout(): Int = 300_000

    @Bean
    fun connectionRequestTimeout(): Int = 300_000

    @Bean
    fun socketTimeout(): Int = 300_000

    @Bean
    fun dksUrl(): String = "https://dks:8443"
}
