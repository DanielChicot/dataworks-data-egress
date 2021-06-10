package uk.gov.dwp.dataworks.egress.services

import uk.gov.dwp.dataworks.egress.domain.EgressSpecification

interface DbService {
    suspend fun tableEntries(prefix: String): List<EgressSpecification>
}
