package uk.gov.dwp.dataworks.egress.provider.impl

import org.apache.http.client.config.RequestConfig
import org.apache.http.conn.ssl.SSLConnectionSocketFactory
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClients
import org.apache.http.ssl.SSLContexts
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import uk.gov.dwp.dataworks.egress.provider.HttpClientProvider
import java.io.File
import javax.net.ssl.SSLContext


@Component
class SecureHttpClientProvider : HttpClientProvider {

    override fun client(): CloseableHttpClient =
        HttpClients.custom().run {
            setDefaultRequestConfig(requestConfig())
            setSSLSocketFactory(connectionFactory())
            build()
        }


    private fun requestConfig(): RequestConfig =
        RequestConfig.custom().run {
            setConnectTimeout(connectTimeout.toInt())
            setConnectionRequestTimeout(connectionRequestTimeout.toInt())
            setSocketTimeout(socketTimeout.toInt())
            build()
        }


    private fun connectionFactory() = SSLConnectionSocketFactory(
        sslContext(),
        arrayOf("TLSv1.2"),
        null,
        SSLConnectionSocketFactory.getDefaultHostnameVerifier())

    private fun sslContext(): SSLContext =
        SSLContexts.custom().run {
            loadKeyMaterial(
                File(identityStore),
                identityStorePassword.toCharArray(),
                identityKeyPassword.toCharArray()) { _, _ -> identityStoreAlias }
            loadTrustMaterial(File(trustStore), trustStorePassword.toCharArray())
            build()
        }

    @Value("\${security.identityStore}")
    private lateinit var identityStore: String

    @Value("\${security.identityStorePassword}")
    private lateinit var identityStorePassword: String

    @Value("\${security.identityStoreAlias}")
    private lateinit var identityStoreAlias: String

    @Value("\${security.identityKeyPassword}")
    private lateinit var identityKeyPassword: String

    @Value("\${security.trustStore}")
    private lateinit var trustStore: String

    @Value("\${security.trustStorePassword}")
    private lateinit var trustStorePassword: String

    @Value("\${security.connectTimeout:300000}")
    private lateinit var connectTimeout: String

    @Value("\${security.connectionRequestTimeout:300000}")
    private lateinit var connectionRequestTimeout: String

    @Value("\${security.socketTimeout:300000}")
    private lateinit var socketTimeout: String

}
