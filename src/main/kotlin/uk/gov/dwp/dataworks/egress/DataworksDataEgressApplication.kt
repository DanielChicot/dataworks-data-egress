package uk.gov.dwp.dataworks.egress

import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.runBlocking
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import software.amazon.awssdk.services.sqs.model.DeleteMessageResponse
import uk.gov.dwp.dataworks.egress.services.DbService
import uk.gov.dwp.dataworks.egress.services.QueueService

@SpringBootApplication
class DataworksDataEgressApplication(private val queueService: QueueService,
									private val dbService: DbService): CommandLineRunner {
	override fun run(vararg args: String?) {
		runBlocking {
			queueService.incomingPrefixes()
				.map { (receiptHandle, prefixes) -> Pair(receiptHandle, prefixes.map {  Pair(it, dbService.tableEntries(it)) }) }
				.onEach(::println)
				.map { it.first }
				.map(queueService::deleteMessage)
				.map(DeleteMessageResponse::responseMetadata)
				.collect(::println)

		}
	}
}

fun main(args: Array<String>) {
	runApplication<DataworksDataEgressApplication>(*args)
}
