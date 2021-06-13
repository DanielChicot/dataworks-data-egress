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
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.SendMessageRequest
import uk.gov.dwp.dataworks.egress.services.CipherService
import uk.gov.dwp.dataworks.egress.services.DataKeyService
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.text.SimpleDateFormat
import java.time.Duration
import java.util.*
import java.util.zip.GZIPInputStream
import java.util.zip.Inflater
import kotlin.time.ExperimentalTime
import com.amazonaws.services.s3.model.PutObjectRequest as PutObjectRequestVersion1


@ExperimentalTime
class IntegrationTests: StringSpec() {



    init {

        "Should process client-side-encrypted encrypted files" {
            val identifier = "cse"
            val sourceContents = sourceContents(identifier)
            val inputStream = ByteArrayInputStream(sourceContents.toByteArray())
            val putRequest =
                PutObjectRequestVersion1(SOURCE_BUCKET, "$identifier/$identifier.csv", inputStream, ObjectMetadata())
            encryptingS3.putObject(putRequest)
            verifyEgress(sourceContents, identifier)
        }

        "Should process htme encrypted files" {
            val identifier = "htme"
            val sourceContents = sourceContents(identifier)
            val (encryptingKeyId, plaintextDataKey, ciphertextDataKey) = dataKeyService.batchDataKey()
            val (iv, encrypted) = cipherService.encrypt(plaintextDataKey, sourceContents.toByteArray())
            val putRequest = with(PutObjectRequest.builder()) {
                bucket(SOURCE_BUCKET)
                key("$identifier/$identifier.csv.enc")
                metadata(mapOf("datakeyencryptionkeyid" to encryptingKeyId,
                    "iv" to iv,
                    "ciphertext" to ciphertextDataKey))
                build()
            }
            s3.putObject(putRequest, AsyncRequestBody.fromBytes(encrypted)).await()
            verifyEgress(sourceContents, identifier)
        }

        "Should process files with today's date in prefix" {
            val identifier = "today"
            val sourceContents = sourceContents(identifier)
            val putRequest = with(PutObjectRequest.builder()) {
                bucket(SOURCE_BUCKET)
                key("$identifier/${todaysDate()}/$identifier.csv")
                build()
            }
            s3.putObject(putRequest, AsyncRequestBody.fromString(sourceContents)).await()
            insertEgressItem("$identifier/\$TODAYS_DATE", "$identifier/\$TODAYS_DATE")
            val message = messageBody("$identifier/${todaysDate()}/$PIPELINE_SUCCESS_FLAG")
            val request = sendMessageRequest(message)
            sqs.sendMessage(request).await()

            withTimeout(Duration.ofSeconds(20)) {
                val targetContents = egressedContents(DESTINATION_BUCKET, "$identifier/${todaysDate()}/$identifier.csv")
                String(targetContents) shouldBe sourceContents
            }
        }

        "Should gz compress files if specified" {
            val identifier = "gz"
            val sourceContents = sourceContents(identifier)
            val putRequest = with(PutObjectRequest.builder()) {
                bucket(SOURCE_BUCKET)
                key("$identifier/$identifier.csv")
                build()
            }
            s3.putObject(putRequest, AsyncRequestBody.fromBytes(sourceContents.toByteArray())).await()
            verifyEgress(sourceContents, identifier, "gz")
        }


        "Should deflate files if specified" {
            val identifier = "z"
            val sourceContents = sourceContents(identifier)
            val putRequest = with(PutObjectRequest.builder()) {
                bucket(SOURCE_BUCKET)
                key("$identifier/$identifier.csv")
                build()
            }
            s3.putObject(putRequest, AsyncRequestBody.fromBytes(sourceContents.toByteArray())).await()
            verifyEgress(sourceContents, identifier, "z")
        }
    }

    private suspend fun verifyEgress(sourceContents: String, identifier: String, compressionFormat: String = "") {
        insertEgressItem("$identifier/", "$identifier/", compressionFormat)
        val message = messageBody("$identifier/$PIPELINE_SUCCESS_FLAG")
        val request = sendMessageRequest(message)
        sqs.sendMessage(request).await()

        withTimeout(Duration.ofSeconds(20)) {
            val targetContents = egressedContents(DESTINATION_BUCKET,
                if (compressionFormat.isEmpty()) "$identifier/$identifier.csv" else "$identifier/$identifier.csv.$compressionFormat")

            if (compressionFormat.isNotEmpty()) {
                if (compressionFormat == "gz") {
                    val output = ByteArrayOutputStream()
                    GZIPInputStream(ByteArrayInputStream(targetContents)).use {
                        it.copyTo(output)
                    }
                    String(output.toByteArray()) shouldBe sourceContents
                } else if (compressionFormat == "z") {
                    with(Inflater()) {
                        setInput(targetContents, 0, targetContents.size)
                        val result = ByteArray(1_000_000)
                        val resultLength = inflate(result)
                        end()
                        String(result, 0, resultLength) shouldBe sourceContents
                    }
                }
            } else {
                String(targetContents) shouldBe sourceContents
            }
        }
    }

    private fun sendMessageRequest(message: String): SendMessageRequest =
        with(SendMessageRequest.builder()) {
            queueUrl("http://localstack:4566/000000000000/integration-queue")
            messageBody(message)
            build()
        }

    private fun messageBody(key: String) =
        """{ "Records": [ { "s3": { "object": { "key": "$key" } } } ] }""".trimIndent()

    private fun sourceContents(style: String) =
        List(100) { "$style,ENCRYPTED,CBOL,REPORT,LINE,NUMBER,$it" }.joinToString("\n")

    private tailrec suspend fun egressedContents(bucket: String, key: String): ByteArray {
        try {
            val request = with(GetObjectRequest.builder()) {
                bucket(bucket)
                key(key)
                build()
            }
            return s3.getObject(request, AsyncResponseTransformer.toBytes()).await().asByteArray()
        } catch (e: NoSuchKeyException) {
            logger.info("'$bucket/$key' not present")
            delay(2000)
        }
        return egressedContents(bucket, key)
    }

    private fun egressColumn(column: String, value: String) =
        column to with(AttributeValue.builder()) {
            s(value)
            build()
        }


    private suspend fun insertEgressItem(sourcePrefix: String, destinationPrefix: String,
                                         compressionFormat: String = ""): PutItemResponse {
        val baseRecord = mapOf<String, AttributeValue>(
            egressColumn(SOURCE_BUCKET_FIELD_NAME, SOURCE_BUCKET),
            egressColumn(DESTINATION_BUCKET_FIELD_NAME, DESTINATION_BUCKET),
            egressColumn(PIPELINE_FIELD_NAME, PIPELINE_NAME),
            egressColumn(SOURCE_PREFIX_FIELD_NAME, sourcePrefix),
            egressColumn(DESTINATION_PREFIX_FIELD_NAME, destinationPrefix),
            egressColumn(TRANSFER_TYPE_FIELD_NAME, TRANSFER_TYPE))

        val record = baseRecord.let { r ->
            compressionFormat.takeIf(String::isNotBlank)?.let { format ->
                r + egressColumn(COMPRESSION_FORMAT_FIELD_NAME, format) +
                        Pair(COMPRESS_FIELD_NAME, AttributeValue.builder().bool(true).build())
            }
        } ?: baseRecord

        val request = with(PutItemRequest.builder()) {
            tableName(EGRESS_TABLE)
            item(record)
            build()
        }
        return dynamoDb.putItem(request).await()
    }

    companion object {

        private val logger = LoggerFactory.getLogger(IntegrationTests::class.java)
        private const val EGRESS_TABLE = "data-egress"
        private const val PIPELINE_NAME = "INTEGRATION_TESTS"
        private const val PIPELINE_SUCCESS_FLAG = "pipeline_success.flag"
        private const val SOURCE_BUCKET = "source"
        private const val DESTINATION_BUCKET = "destination"
        private const val TRANSFER_TYPE = "S3"

        private const val SOURCE_BUCKET_FIELD_NAME = "source_bucket"
        private const val DESTINATION_BUCKET_FIELD_NAME = "destination_bucket"
        private const val PIPELINE_FIELD_NAME = "pipeline_name"
        private const val SOURCE_PREFIX_FIELD_NAME = "source_prefix"
        private const val DESTINATION_PREFIX_FIELD_NAME = "destination_prefix"
        private const val TRANSFER_TYPE_FIELD_NAME = "transfer_type"
        private const val COMPRESSION_FORMAT_FIELD_NAME = "compress_fmt"
        private const val COMPRESS_FIELD_NAME = "compress"

        private val applicationContext by lazy {
            AnnotationConfigApplicationContext(TestConfiguration::class.java)
        }

        private val sqs = applicationContext.getBean(SqsAsyncClient::class.java)
        private val encryptingS3 = applicationContext.getBean(AmazonS3EncryptionV2::class.java)
        private val s3 = applicationContext.getBean(S3AsyncClient::class.java)
        private val dynamoDb = applicationContext.getBean(DynamoDbAsyncClient::class.java)
        private val cipherService = applicationContext.getBean(CipherService::class.java)
        private val dataKeyService = applicationContext.getBean(DataKeyService::class.java)

        private fun todaysDate() = SimpleDateFormat("yyyy-MM-dd").format(Date())
    }
}
