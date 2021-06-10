package uk.gov.dwp.dataworks.egress.services.impl

import kotlinx.coroutines.future.await
import org.springframework.stereotype.Service
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ScanRequest
import uk.gov.dwp.dataworks.egress.domain.EgressSpecification
import uk.gov.dwp.dataworks.egress.services.DbService
import java.text.SimpleDateFormat
import java.util.*

@Service
class DbServiceImpl(private val dynamoDb: DynamoDbAsyncClient,
                    private val dataEgressTable: String): DbService {

    override suspend fun tableEntries(prefix: String): List<EgressSpecification> =
        entries().filter {
            it[SOURCE_PREFIX_COLUMN]?.s()
                ?.replace(TODAYS_DATE_PLACEHOLDER, todaysDate())
                ?.replace(Regex("""\*$"""), "")?.let(prefix::startsWith) ?: false
        }.map {
            EgressSpecification(
                sourceBucket = attributeStringValue(it, SOURCE_BUCKET_COLUMN),
                sourcePrefix = attributeStringValue(it, SOURCE_PREFIX_COLUMN).replace(TODAYS_DATE_PLACEHOLDER, todaysDate()),
                destinationBucket = attributeStringValue(it, DESTINATION_BUCKET_COLUMN),
                destinationPrefix = attributeStringValue(it, DESTINATION_PREFIX_COLUMN).replace(TODAYS_DATE_PLACEHOLDER, todaysDate()),
                transferType = attributeStringValue(it, TRANSFER_TYPE_COLUMN),
                compress = it[COMPRESS_COLUMN]?.bool() ?: false,
                compressionFormat = it[COMPRESSION_FORMAT_COLUMN]?.s(),
                roleArn = it[ROLE_ARN_COLUMN]?.s())
        }

    private fun attributeStringValue(it: Map<String, AttributeValue>, key: String) = it[key]?.s() ?: ""

    private tailrec suspend fun entries(accumulated: List<Map<String, AttributeValue>> = emptyList(),
                                        startKey: Map<String, AttributeValue>? = null): List<Map<String, AttributeValue>> {
        val response = dynamoDb.scan(scanRequest(startKey)).await()
        val nextPage = response.items()
        val lastKey = response.lastEvaluatedKey()

        return if (lastKey == null || lastKey.isEmpty())
            accumulated + nextPage
        else
            entries(accumulated + nextPage, lastKey)
    }

    private fun scanRequest(startKey: Map<String, AttributeValue>?): ScanRequest =
        with(ScanRequest.builder()) {
            tableName(dataEgressTable)
            startKey?.let {
                exclusiveStartKey(startKey)
            }
            build()
        }

    private fun todaysDate() = SimpleDateFormat("yyyy-MM-dd").format(Date())

    companion object {
        private const val SOURCE_PREFIX_COLUMN: String = "source_prefix"
        private const val SOURCE_BUCKET_COLUMN: String = "source_bucket"
        private const val DESTINATION_BUCKET_COLUMN: String = "destination_bucket"
        private const val DESTINATION_PREFIX_COLUMN: String = "destination_prefix"
        private const val TRANSFER_TYPE_COLUMN: String = "transfer_type"
        private const val COMPRESS_COLUMN: String = "compress"
        private const val COMPRESSION_FORMAT_COLUMN: String = "compress_fmt"
        private const val ROLE_ARN_COLUMN: String = "role_arn"
        private const val TODAYS_DATE_PLACEHOLDER = "\$TODAYS_DATE"
    }
}
