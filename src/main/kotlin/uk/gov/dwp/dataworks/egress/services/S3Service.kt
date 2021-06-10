package uk.gov.dwp.dataworks.egress.services

import uk.gov.dwp.dataworks.egress.domain.EgressSpecification

interface S3Service {
    suspend fun egressObject(key: String, specification: EgressSpecification): Boolean
    suspend fun egressObjects(key: String, specifications: List<EgressSpecification>): Boolean
}
