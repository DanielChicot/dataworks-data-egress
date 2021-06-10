package uk.gov.dwp.dataworks.egress.services

import uk.gov.dwp.dataworks.egress.domain.EncryptionResult
import java.io.OutputStream
import java.security.Key
import javax.crypto.Cipher

interface CipherService {
    fun decrypt(key: String, initializationVector: String, encrypted: ByteArray): ByteArray
    fun encrypt(key: String, plaintext: ByteArray): EncryptionResult
}
