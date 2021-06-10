package uk.gov.dwp.dataworks.egress

import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import org.springframework.context.annotation.PropertySource
import uk.gov.dwp.dataworks.egress.configuration.ContextConfiguration
import uk.gov.dwp.dataworks.egress.configuration.LocalstackConfiguration
import uk.gov.dwp.dataworks.egress.properties.AwsProperties
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
    CipherServiceImpl::class)
@Configuration
@PropertySource("classpath:integration.properties")
class TestConfiguration
