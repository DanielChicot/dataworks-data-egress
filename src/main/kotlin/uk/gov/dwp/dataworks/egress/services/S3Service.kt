package uk.gov.dwp.dataworks.egress.services

interface S3Service {
    fun putObject()
    fun putObjectWithAssumedRole()
}
