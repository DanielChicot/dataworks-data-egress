package uk.gov.dwp.dataworks.egress

import com.amazonaws.services.s3.AmazonS3EncryptionV2
import com.amazonaws.services.s3.model.ObjectMetadata
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import kotlinx.coroutines.time.withTimeout
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.SendMessageRequest
import uk.gov.dwp.dataworks.egress.services.CipherService
import uk.gov.dwp.dataworks.egress.services.DataKeyService
import java.io.ByteArrayInputStream
import java.nio.charset.Charset
import java.text.SimpleDateFormat
import java.time.Duration
import java.util.*
import kotlin.time.ExperimentalTime
import com.amazonaws.services.s3.model.PutObjectRequest as PutObjectRequestVersion1


@ExperimentalTime
class IntegrationTests: StringSpec() {
    init {
        "Should process EMR encrypted files" {
            val sourceContents = sourceContents("EMRFS")
            val inputStream = ByteArrayInputStream(sourceContents.toByteArray())
            val putRequest = PutObjectRequestVersion1("source", cbolReportKey(), inputStream, ObjectMetadata())
            encryptingS3.putObject(putRequest)
            val message = messageBody(cbolReportKey())
            val request = sendMessageRequest(message)
            sqs.sendMessage(request).await()

            withTimeout(Duration.ofSeconds(20)) {
                val targetContents = egressedContents("target", "cbol/cbol.csv")
                targetContents shouldBe sourceContents
            }
        }

        "Should process HTME encrypted files" {
            val sourceContents = sourceContents("HTME")
            val (encryptingKeyId, plaintextDataKey, ciphertextDataKey) = dataKeyService.batchDataKey()
            val (iv, encrypted) = cipherService.encrypt(plaintextDataKey, sourceContents.toByteArray())
            val putRequest = with (PutObjectRequest.builder()) {
                bucket("source")
                key("opsmi/opsmi.csv.enc")
                metadata(mapOf("datakeyencryptionkeyid" to encryptingKeyId, "iv" to iv, "ciphertext" to ciphertextDataKey))
                build()
            }
            s3.putObject(putRequest, AsyncRequestBody.fromBytes(encrypted)).await()
            val message = messageBody("opsmi/opsmi.csv")
            val request = sendMessageRequest(message)
            sqs.sendMessage(request).await()

            withTimeout(Duration.ofSeconds(20)) {
                egressedContents("target", "opsmi.csv") shouldBe sourceContents
            }
        }
    }

    private fun sendMessageRequest(message: String): SendMessageRequest =
        with(SendMessageRequest.builder()) {
            queueUrl("http://localstack:4566/000000000000/integration-queue")
            messageBody(message)
            build()
        }

    private fun messageBody(key: String) = """{ "Records": [ { "s3": { "object": { "key": "$key" } } } ] }""".trimIndent()

    private fun sourceContents(style: String) = List(100) { "$style,ENCRYPTED,CBOL,REPORT,LINE,NUMBER,$it" }.joinToString("\n")

    private tailrec suspend fun egressedContents(bucket: String, key: String): String {
        try {
            val request = with(GetObjectRequest.builder()) {
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
        private val cipherService = applicationContext.getBean(CipherService::class.java)
        private val dataKeyService = applicationContext.getBean(DataKeyService::class.java)
        private fun todaysDate() = SimpleDateFormat("yyyy-MM-dd").format(Date())
    }
}
