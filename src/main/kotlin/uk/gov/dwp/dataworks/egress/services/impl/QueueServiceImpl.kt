package uk.gov.dwp.dataworks.egress.services.impl

import com.google.gson.Gson
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.future.asDeferred
import kotlinx.coroutines.future.await
import org.springframework.stereotype.Service
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.*
import uk.gov.dwp.dataworks.egress.services.QueueService
import uk.gov.dwp.dataworks.logging.DataworksLogger
import kotlin.time.ExperimentalTime

@ExperimentalTime
@Service
class QueueServiceImpl(private val sqs: SqsAsyncClient,
                       private val sqsQueueUrl: String,
                       private val sqsCheckIntervalMs: Long): QueueService {

    override fun incomingPrefixes(): Flow<Pair<String, List<String>>> =
        flow {
            while (true) {
                if (messageCount() > 0) {
                    val response = sqs.receiveMessage(receiveMessageRequest()).await()
                    if (response.hasMessages() && response.messages().size > 0) {
                        val message = response.messages().first()
                        val receiptHandle = message.receiptHandle()
                        sqs.changeMessageVisibility(changeMessageVisibilityRequest(receiptHandle)).await()
                        logger.info("Message received", "body" to escaped(message))
                        val body = gson.jsonObject(message.body())
                        if (body.has("Records")) {
                            emit(Pair(receiptHandle, messagePrefixes(body)))
                        }
                    }
                } else {
                    logger.info("Nothing on the queue")
                    delay(sqsCheckIntervalMs)
                }
            }
        }

    private fun receptions(attributes: MutableMap<MessageSystemAttributeName, String>): Int =
        (attributes[MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT] ?: "1").toInt()


    override suspend fun deleteMessage(receiptHandle: String): DeleteMessageResponse =
        sqs.deleteMessage(deleteMessageRequest(receiptHandle)).asDeferred().await()

    private fun deleteMessageRequest(receiptHandle: String): DeleteMessageRequest =
        with(DeleteMessageRequest.builder()) {
            queueUrl(sqsQueueUrl)
            receiptHandle(receiptHandle)
            build()
        }

    private fun changeMessageVisibilityRequest(receipt: String): ChangeMessageVisibilityRequest =
        with(ChangeMessageVisibilityRequest.builder()) {
            queueUrl(sqsQueueUrl)
            receiptHandle(receipt)
            visibilityTimeout(15 * 60)
            build()
        }

    private fun messagePrefixes(body: JsonObject): List<String> =
        body.getAsJsonArray("Records")
            .asSequence()
            .map(JsonElement::getAsJsonObject)
            .filter { it.has("s3") }
            .map { it.getAsJsonObject("s3") }
            .filter { it.has("object") }
            .map { it.getAsJsonObject("object") }
            .filter { it.has("key") }
            .map { it.getAsJsonPrimitive("key") }
            .map { it.asString }
            .toList()

    private fun Gson.jsonObject(s: String): JsonObject = this.fromJson(s, JsonObject::class.java)

    private fun receiveMessageRequest(): ReceiveMessageRequest =
        ReceiveMessageRequest.builder()
            .queueUrl(sqsQueueUrl)
            .maxNumberOfMessages(1)
            .build()

    private suspend fun messageCount(): Int =
        queueAttributes()[QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES]?.toInt() ?: 0

    private suspend fun queueAttributes(): Map<QueueAttributeName, String> =
        sqs.getQueueAttributes(getQueueAttributesRequest()).await().attributes()

    private fun getQueueAttributesRequest(): GetQueueAttributesRequest? =
        with(GetQueueAttributesRequest.builder()) {
            queueUrl(sqsQueueUrl)
            attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES)
            build()
        }

    private fun escaped(message: Message): String =
        with(JsonObject()) {
            addProperty("message", message.body())
            Gson().toJson(this["message"])
        }

    companion object {
        private val gson = Gson()
        private val logger: DataworksLogger = DataworksLogger.getLogger(QueueServiceImpl::class.java)
    }

}
