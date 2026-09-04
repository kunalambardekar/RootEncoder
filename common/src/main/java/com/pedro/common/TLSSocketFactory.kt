/*
 * Copyright (C) 2023 pedroSG94.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pedro.common

import java.io.IOException
import java.net.InetAddress
import java.net.Socket
import java.net.UnknownHostException
import java.security.SecureRandom
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLSocket
import javax.net.ssl.SSLSocketFactory
import javax.net.ssl.TrustManager

/**
 * @author fkrauthan
 */
open class TLSSocketFactory(
  trustManagers: Array<TrustManager>? = null
): SSLSocketFactory() {

  private val internalSSLSocketFactory: SSLSocketFactory

  init {
    val context = SSLContext.getInstance("TLS")
    val secureRandom = if (trustManagers != null) SecureRandom() else null
    context.init(null, trustManagers, secureRandom)
    internalSSLSocketFactory = context.socketFactory
  }

  override fun getDefaultCipherSuites(): Array<String> {
    return internalSSLSocketFactory.defaultCipherSuites
  }

  override fun getSupportedCipherSuites(): Array<String> {
    return internalSSLSocketFactory.supportedCipherSuites
  }

  @Throws(IOException::class)
  override fun createSocket(): Socket {
    return enableTLSOnSocket(internalSSLSocketFactory.createSocket())
  }

  @Throws(IOException::class)
  override fun createSocket(s: Socket, host: String, port: Int, autoClose: Boolean): Socket {
    return enableTLSOnSocket(internalSSLSocketFactory.createSocket(s, host, port, autoClose))
  }

  @Throws(IOException::class, UnknownHostException::class)
  override fun createSocket(host: String, port: Int): Socket {
    return enableTLSOnSocket(internalSSLSocketFactory.createSocket(host, port))
  }

  @Throws(IOException::class, UnknownHostException::class)
  override fun createSocket(host: String, port: Int, localHost: InetAddress, localPort: Int): Socket {
    return enableTLSOnSocket(internalSSLSocketFactory.createSocket(host, port, localHost, localPort))
  }

  @Throws(IOException::class)
  override fun createSocket(host: InetAddress, port: Int): Socket {
    return enableTLSOnSocket(internalSSLSocketFactory.createSocket(host, port))
  }

  @Throws(IOException::class)
  override fun createSocket(address: InetAddress, port: Int, localAddress: InetAddress, localPort: Int): Socket {
    return enableTLSOnSocket(internalSSLSocketFactory.createSocket(address, port, localAddress, localPort))
  }

  private fun enableTLSOnSocket(socket: Socket): Socket {
    if (socket is SSLSocket) {
      // versus fork: TLSv1.1 was removed from the platform on Android 14+/16, and asking for a
      // protocol the socket no longer supports makes setEnabledProtocols throw ("protocol TLSv1.1
      // is not supported"), killing the RTMPS handshake. Enable only the modern protocols the
      // socket actually supports — TLS 1.2 (always) and TLS 1.3 (API 29+).
      val versusDesired = setOf("TLSv1.2", "TLSv1.3")
      socket.enabledProtocols = socket.supportedProtocols.filter { it in versusDesired }.toTypedArray()
    }
    return socket
  }
}