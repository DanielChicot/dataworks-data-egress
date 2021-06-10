package uk.gov.dwp.dataworks.egress

import com.amazonaws.services.s3.AmazonS3EncryptionV2
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import io.kotest.core.spec.style.StringSpec
import kotlinx.coroutines.future.await
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.SendMessageRequest
import java.io.ByteArrayInputStream
import java.text.SimpleDateFormat
import java.util.*

class IntegrationTests: StringSpec() {
    init {
        "Should be able to process files encrypted by EMR" {
            println("encryptingS3: '$encryptingS3'.")
            val contents = List(100) {"CBOL,REPORT,LINE,NUMBER,$it"}.joinToString("\n")
            val inputStream = ByteArrayInputStream(contents.toByteArray())
            val putRequest  = PutObjectRequest("source", cbolReportKey(), inputStream, ObjectMetadata())
            encryptingS3.putObject(putRequest)

            val message = """{
                "Records": [
                    { "s3": { "object": { "key": "${cbolReportKey()}" } } }
                ]
            }"""

            val request = with(SendMessageRequest.builder()) {
                queueUrl("http://localstack:4566/000000000000/integration-queue")
                messageBody(message)
                build()
            }

            val response = sqs.sendMessage(request).await()
        }
    }

    private fun cbolReportKey() = """dataegress/cbol-report/${todaysDate()}/cbol.csv"""


    companion object {

        private val applicationContext by lazy {
            AnnotationConfigApplicationContext(TestConfiguration::class.java)
        }

        private val sqs = applicationContext.getBean(SqsAsyncClient::class.java)
        private val encryptingS3 = applicationContext.getBean(AmazonS3EncryptionV2::class.java)
        private fun todaysDate() = SimpleDateFormat("yyyy-MM-dd").format(Date())
    }
}
