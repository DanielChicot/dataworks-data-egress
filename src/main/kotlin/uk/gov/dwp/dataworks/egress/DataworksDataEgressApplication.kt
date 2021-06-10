package uk.gov.dwp.dataworks.egress

import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.runBlocking
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import uk.gov.dwp.dataworks.egress.domain.EgressSpecification
import uk.gov.dwp.dataworks.egress.services.DbService
import uk.gov.dwp.dataworks.egress.services.QueueService
import uk.gov.dwp.dataworks.egress.services.S3Service
import uk.gov.dwp.dataworks.logging.DataworksLogger

@SpringBootApplication
class DataworksDataEgressApplication(private val queueService: QueueService,
									 private val dbService: DbService,
									 private val s3Service: S3Service): CommandLineRunner {
	override fun run(vararg args: String?) {
		runBlocking {
			queueService.incomingPrefixes()
				.map { (receiptHandle, prefixes) -> Pair(receiptHandle, prefixes.map {  Pair(it, dbService.tableEntries(it)) }) }
				.onEach { (receiptHandle, egressRequests: List<Pair<String, List<EgressSpecification>>>) ->
					egressRequests.map { (key, egressSpecifications) ->
						s3Service.egressObjects(key, egressSpecifications)
					}
				}
//				.map { it.first }
//				.map(queueService::deleteMessage)
//				.map(DeleteMessageResponse::responseMetadata)
				.collect(::println)

		}
	}

	companion object {
		private val logger = DataworksLogger.getLogger(DataworksDataEgressApplication::class)
	}
}

fun main(args: Array<String>) {
	runApplication<DataworksDataEgressApplication>(*args)
}
