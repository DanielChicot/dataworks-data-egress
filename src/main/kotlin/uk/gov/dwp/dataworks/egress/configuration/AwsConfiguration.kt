package uk.gov.dwp.dataworks.egress.configuration

import com.amazonaws.services.s3.AmazonS3EncryptionClientV2
import com.amazonaws.services.s3.AmazonS3EncryptionV2
import com.amazonaws.services.s3.model.CryptoConfigurationV2
import com.amazonaws.services.s3.model.CryptoMode
import com.amazonaws.services.s3.model.EncryptionMaterialsProvider
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.sqs.SqsAsyncClient

@Configuration
@Profile("!LOCALSTACK")
class AwsConfiguration(private val encryptionMaterialsProvider: EncryptionMaterialsProvider) {

    @Bean
    fun encryptingS3Client(): AmazonS3EncryptionV2 =
        AmazonS3EncryptionClientV2.encryptionBuilder()
            .withEncryptionMaterialsProvider(encryptionMaterialsProvider)
            .withCryptoConfiguration(CryptoConfigurationV2().withCryptoMode(CryptoMode.AuthenticatedEncryption))
            .build()

    @Bean
    fun sqsClient(): SqsAsyncClient = SqsAsyncClient.create()

    @Bean
    fun dynamoDbClient(): DynamoDbAsyncClient = DynamoDbAsyncClient.create()
}
