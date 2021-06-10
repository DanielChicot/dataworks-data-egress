package uk.gov.dwp.dataworks.egress.services.impl

import com.amazonaws.services.s3.AmazonS3EncryptionV2
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withContext
import org.springframework.stereotype.Service
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import uk.gov.dwp.dataworks.egress.domain.EgressSpecification
import uk.gov.dwp.dataworks.egress.services.S3Service
import uk.gov.dwp.dataworks.logging.DataworksLogger
import java.io.ByteArrayOutputStream
import com.amazonaws.services.s3.model.GetObjectRequest as GetObjectRequestVersion1

@Service
class S3ServiceImpl(private val s3AsyncClient: S3AsyncClient,
                    private val s3Client: S3Client,
                    private val decryptingS3Client: AmazonS3EncryptionV2,
                    private val assumedRoleS3ClientProvider: (String) -> suspend () -> S3AsyncClient): S3Service {

    override suspend fun egressObjects(key: String, specifications: List<EgressSpecification>) {
        specifications.forEach { specification -> egressObject(key, specification) }
    }

    override suspend fun egressObject(key: String, specification: EgressSpecification) {
        logger.info("Egressing s3 object", "key" to key, "specification" to "$specification")
        val metadata = objectMetadata(specification.sourceBucket, key)
        logger.info("Got metadata", "metadata" to "$metadata")
        if (metadata.containsKey(MATERIALS_DESCRIPTION_METADATA_KEY)) {
            logger.info("Found client side encrypted object")
            val contents = encryptedObjectContents(specification.sourceBucket, key)
            println(String(contents))
        }
        logger.info("AFTER COROUTINE")
    }

    private suspend fun encryptedObjectContents(bucket: String, key: String) = withContext(Dispatchers.IO) {
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
