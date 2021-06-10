package uk.gov.dwp.dataworks.egress.services.impl

import org.springframework.stereotype.Service
import software.amazon.awssdk.services.s3.S3AsyncClient
import uk.gov.dwp.dataworks.egress.services.S3Service

@Service
class S3ServiceImpl(private val s3Client: S3AsyncClient,
                    private val assumedRoleS3ClientProvider: (String) -> suspend () -> S3AsyncClient): S3Service {

    override fun putObject() {

    }

    override fun putObjectWithAssumedRole() {

    }
}
