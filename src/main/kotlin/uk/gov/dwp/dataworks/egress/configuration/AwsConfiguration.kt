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
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sts.StsClient
import uk.gov.dwp.dataworks.egress.configuration.StsUtility.credentialsProvider

@Configuration
@Profile("!LOCALSTACK")
class AwsConfiguration(private val encryptionMaterialsProvider: EncryptionMaterialsProvider) {

    @Bean
    fun decryptingS3Client(): AmazonS3EncryptionV2 =
        AmazonS3EncryptionClientV2.encryptionBuilder()
            .withEncryptionMaterialsProvider(encryptionMaterialsProvider)
            .withCryptoConfiguration(CryptoConfigurationV2().withCryptoMode(CryptoMode.AuthenticatedEncryption))
            .build()

    @Bean
    fun s3Client(): S3Client = S3Client.create()

    @Bean
    fun s3AsyncClient(): S3AsyncClient = S3AsyncClient.create()

    @Bean
    fun sqsClient(): SqsAsyncClient = SqsAsyncClient.create()

    @Bean
    fun dynamoDbClient(): DynamoDbAsyncClient = DynamoDbAsyncClient.create()

    @Bean
    fun assumedRoleS3ClientProvider(): suspend (String) -> S3AsyncClient {
        val stsClient = StsClient.create()
        return { roleArn: String ->
            with(S3AsyncClient.builder()) {
                credentialsProvider(credentialsProvider(stsClient, roleArn))
                build()
            }
        }
    }
}
