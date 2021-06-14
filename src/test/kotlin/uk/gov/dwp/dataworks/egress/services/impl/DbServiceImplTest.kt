package uk.gov.dwp.dataworks.egress.services.impl

import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import io.kotest.core.spec.style.WordSpec
import io.kotest.matchers.collections.shouldContainExactly
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ScanRequest
import software.amazon.awssdk.services.dynamodb.model.ScanResponse
import uk.gov.dwp.dataworks.egress.domain.EgressSpecification
import java.text.SimpleDateFormat
import java.util.*
import java.util.concurrent.CompletableFuture

class DbServiceImplTest: WordSpec() {
    init {
        "DbService" should {
            "return matching item, filter non-matching item" {
                val receivedPrefix = "source/prefix/pipeline_success.flag"
                val matchingPrefix = "source/prefix/"
                val matchingItem = egressTableItem(matchingPrefix)
                val nonMatchingItem = egressTableItem("non/matching/prefix")
                val scanResponse = with(ScanResponse.builder()) {
                    items(matchingItem, nonMatchingItem)
                    build()
                }
                val scanFuture = CompletableFuture.completedFuture(scanResponse)
                val dynamoDb = mock<DynamoDbAsyncClient> {
                    on { scan(any<ScanRequest>()) } doReturn scanFuture
                }
                val dbService = DbServiceImpl(dynamoDb, DATA_EGRESS_TABLE)
                val entries = dbService.tableEntryMatches(receivedPrefix)
                entries shouldContainExactly listOf(egressSpecification(matchingPrefix))
            }

            "match items with today's date" {
                val interpolatedPrefix = "source/prefix/${todaysDate()}"
                val receivedPrefix = "$interpolatedPrefix/pipeline_success.flag"
                val matchingPrefix = "source/prefix/$TODAYS_DATE_PLACEHOLDER/"

                val matchingItem = egressTableItem(matchingPrefix)
                val scanResponse = with(ScanResponse.builder()) {
                    items(matchingItem)
                    build()
                }
                val scanFuture = CompletableFuture.completedFuture(scanResponse)
                val dynamoDb = mock<DynamoDbAsyncClient> {
                    on { scan(any<ScanRequest>()) } doReturn scanFuture
                }
                val dbService = DbServiceImpl(dynamoDb, DATA_EGRESS_TABLE)
                val entries = dbService.tableEntryMatches(receivedPrefix)
                entries shouldContainExactly listOf(egressSpecification("$interpolatedPrefix/"))
            }
        }
    }

    private fun egressSpecification(matchingPrefix: String) = EgressSpecification(
        SOURCE_BUCKET, matchingPrefix, DESTINATION_BUCKET, DESTINATION_PREFIX, TRANSFER_TYPE, false, false, null, null
    )

    private fun egressTableItem(matchingPrefix: String) = mapOf(
        SOURCE_PREFIX_KEY to attributeValue(matchingPrefix),
        DESTINATION_PREFIX_KEY to attributeValue(DESTINATION_PREFIX),
        SOURCE_BUCKET_KEY to attributeValue(SOURCE_BUCKET),
        DESTINATION_BUCKET_KEY to attributeValue(DESTINATION_BUCKET),
        TRANSFER_TYPE_KEY to attributeValue(TRANSFER_TYPE)).toMutableMap()


    companion object {
        private fun attributeValue(matchingPrefix: String) = AttributeValue.builder().s(matchingPrefix).build()
        private fun todaysDate() = SimpleDateFormat("yyyy-MM-dd").format(Date())

        private const val DATA_EGRESS_TABLE = "DATA_EGRESS_TABLE"
        private const val SOURCE_PREFIX_KEY: String = "source_prefix"
        private const val SOURCE_BUCKET_KEY: String = "source_bucket"
        private const val DESTINATION_BUCKET_KEY: String = "destination_bucket"
        private const val DESTINATION_PREFIX_KEY: String = "destination_prefix"
        private const val TRANSFER_TYPE_KEY: String = "transfer_type"
        private const val TODAYS_DATE_PLACEHOLDER = "\$TODAYS_DATE"

        private const val SOURCE_BUCKET = "source"
        private const val DESTINATION_BUCKET = "destination"
        private const val TRANSFER_TYPE = "S3"
        private const val DESTINATION_PREFIX = "destination/prefix"
    }
}
