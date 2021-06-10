package uk.gov.dwp.dataworks.egress.configuration

import kotlinx.coroutines.future.await
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.sts.StsAsyncClient
import software.amazon.awssdk.services.sts.StsClient
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest
import software.amazon.awssdk.services.sts.model.Credentials

@Configuration
class CommonAwsConfiguration {

    @Bean
    fun assumedRoleS3ClientProvider(): suspend (String) -> S3AsyncClient =
        { roleArn: String ->
            val stsCredentialsProvider = with (StsAssumeRoleCredentialsProvider.builder()) {
                refreshRequest(assumeRoleRequest(roleArn))
                stsClient(stsClient())
                build()
            }

            with(S3AsyncClient.builder()) {
                credentialsProvider(stsCredentialsProvider)
                build()
            }
        }

    private fun assumeRoleRequest(arn: String): AssumeRoleRequest =
        with(AssumeRoleRequest.builder()) {
            roleArn(arn)
            roleSessionName("data-egress")
            build()
        }

    private fun stsClient(): StsClient = StsClient.create()
}
