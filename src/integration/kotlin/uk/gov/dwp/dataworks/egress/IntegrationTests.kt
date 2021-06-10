package uk.gov.dwp.dataworks.egress

import com.amazonaws.services.s3.AmazonS3EncryptionV2
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import kotlinx.coroutines.time.withTimeout
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import software.amazon.awssdk.core.ResponseBytes
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.SendMessageRequest
import java.io.ByteArrayInputStream
import java.nio.charset.Charset
import java.text.SimpleDateFormat
import java.time.Duration
import java.util.*
import kotlin.time.ExperimentalTime


@ExperimentalTime
class IntegrationTests: StringSpec() {
    init {
        "Should be able to process files encrypted by EMR" {
            val sourceContents = List(100) {"EMRFS,ENCRYPTED,CBOL,REPORT,LINE,NUMBER,$it"}.joinToString("\n")
            val inputStream = ByteArrayInputStream(sourceContents.toByteArray())
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

            sqs.sendMessage(request).await()

            withTimeout(Duration.ofSeconds(20)) {
                val targetContents = egressedContents("target", "cbol/cbol.csv")
                targetContents shouldBe sourceContents
            }
        }
    }

    private tailrec suspend fun egressedContents(bucket: String, key: String): String {
        try {
            val request = with (GetObjectRequest.builder()) {
                bucket(bucket)
                key(key)
                build()
            }
            val targetContents = s3.getObject(request, AsyncResponseTransformer.toBytes()).await()
            return targetContents.asString(Charset.defaultCharset())
        } catch (e: NoSuchKeyException) {
            logger.info("'$bucket/$key' not present")
            delay(2000)
        }
        return egressedContents(bucket, key)
    }

    private fun cbolReportKey() = """dataegress/cbol-report/${todaysDate()}/cbol.csv"""


    companion object {

        private val logger = LoggerFactory.getLogger(IntegrationTests::class.java)

        private val applicationContext by lazy {
            AnnotationConfigApplicationContext(TestConfiguration::class.java)
        }

        private val sqs = applicationContext.getBean(SqsAsyncClient::class.java)
        private val encryptingS3 = applicationContext.getBean(AmazonS3EncryptionV2::class.java)
        private val s3 = applicationContext.getBean(S3AsyncClient::class.java)
        private fun todaysDate() = SimpleDateFormat("yyyy-MM-dd").format(Date())
    }
}
