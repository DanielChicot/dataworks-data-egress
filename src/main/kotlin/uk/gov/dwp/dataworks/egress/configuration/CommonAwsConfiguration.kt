package uk.gov.dwp.dataworks.egress.configuration

import kotlinx.coroutines.future.await
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.sts.StsAsyncClient
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest
import software.amazon.awssdk.services.sts.model.Credentials

@Configuration
class CommonAwsConfiguration {

    @Bean
    fun assumedRoleS3ClientProvider(stsAsyncClient: StsAsyncClient): (String) -> suspend () -> S3AsyncClient {
        return { roleArn: String ->
            suspend {
                val credentials: Credentials = credentials(stsAsyncClient, roleArn)
                with(S3AsyncClient.builder()) {
                    credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(credentials.accessKeyId(), credentials.secretAccessKey())))
                    build()
                }
            }
        }
    }

    private suspend fun credentials(stsAsyncClient: StsAsyncClient, arn: String): Credentials =
            stsAsyncClient.assumeRole(assumeRoleRequest(arn)).await().credentials()

    private fun assumeRoleRequest(arn: String): AssumeRoleRequest =
        with(AssumeRoleRequest.builder()) {
            roleArn(arn)
            build()
        }
}
