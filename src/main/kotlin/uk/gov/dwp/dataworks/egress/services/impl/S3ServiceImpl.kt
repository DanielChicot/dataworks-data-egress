package uk.gov.dwp.dataworks.egress.services.impl

import com.amazonaws.services.s3.AmazonS3EncryptionV2
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.future.await
import kotlinx.coroutines.withContext
import org.springframework.stereotype.Service
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import uk.gov.dwp.dataworks.egress.domain.EgressSpecification
import uk.gov.dwp.dataworks.egress.services.S3Service
import uk.gov.dwp.dataworks.logging.DataworksLogger
import java.io.ByteArrayOutputStream
import java.io.File
import com.amazonaws.services.s3.model.GetObjectRequest as GetObjectRequestVersion1

@Service
class S3ServiceImpl(private val s3AsyncClient: S3AsyncClient,
                    private val s3Client: S3Client,
                    private val decryptingS3Client: AmazonS3EncryptionV2,
                    private val assumedRoleS3ClientProvider: suspend (String) -> S3AsyncClient): S3Service {

    override suspend fun egressObjects(key: String, specifications: List<EgressSpecification>): Boolean =
        specifications.map { specification -> egressObject(key, specification) }.all { it }

    override suspend fun egressObject(key: String, specification: EgressSpecification): Boolean =
        try {
            logger.info("Egressing s3 object", "key" to key, "specification" to "$specification")
            val metadata = objectMetadata(specification.sourceBucket, key)
            logger.info("Got metadata", "metadata" to "$metadata")
            val sourceContents = sourceContents(metadata, specification, key)
            val targetContents = targetContents(metadata, specification, sourceContents)
            egressClient(specification)
                .putObject(putObjectRequest(specification, key), AsyncRequestBody.fromBytes(targetContents)).await()
            true
        } catch (e: Exception) {
            logger.error("Failed to egress object", e, "key" to key, "specification" to "$specification")
            false
        }


    private fun putObjectRequest(specification: EgressSpecification,
                                 key: String): PutObjectRequest =
        with(PutObjectRequest.builder()) {
            bucket(specification.destinationBucket)
            key("${specification.destinationPrefix.replace(Regex("""/$"""), "")}/${File(key).name}")
            build()
        }

    private suspend fun egressClient(specification: EgressSpecification): S3AsyncClient =
        specification.roleArn?.let {
            assumedRoleS3ClientProvider(specification.roleArn)
        } ?: s3AsyncClient

    private fun targetContents(metadata: Map<String, String>,
                               specification: EgressSpecification,
                               sourceContents: ByteArray): ByteArray {
        return sourceContents
    }

    private suspend fun sourceContents(metadata: MutableMap<String, String>,
                                       specification: EgressSpecification,
                                       key: String): ByteArray =
        when {
            wasEmrfsClientSideEncrypted(metadata) -> {
                logger.info("Found client side encrypted object")
                decryptedObjectContents(specification.sourceBucket, key)
            }
            else -> {
                ByteArray(0)
            }
        }

    private fun wasEmrfsClientSideEncrypted(metadata: MutableMap<String, String>) =
        metadata.containsKey(MATERIALS_DESCRIPTION_METADATA_KEY)

    private suspend fun decryptedObjectContents(bucket: String, key: String) = withContext(Dispatchers.IO) {
        val outputStream = ByteArrayOutputStream()
        decryptingS3Client.getObject(GetObjectRequestVersion1(bucket, key)).objectContent.use {
            it.copyTo(outputStream)
        }
        outputStream.toByteArray()
    }

    private suspend fun objectMetadata(bucket: String, key: String) = withContext(Dispatchers.IO) {
        val getRequest = getObjectRequest(bucket, key)
        s3Client.getObject(getRequest).response().metadata()
    }

    private fun getObjectRequest(bucket: String, key: String): GetObjectRequest =
        with(GetObjectRequest.builder()) {
            bucket(bucket)
            key(key)
            build()
        }

    companion object {
        private val logger = DataworksLogger.getLogger(S3ServiceImpl::class)
        private const val MATERIALS_DESCRIPTION_METADATA_KEY = "x-amz-matdesc"
    }
}
