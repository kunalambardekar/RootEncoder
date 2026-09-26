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

package com.pedro.rtmp.rtmp

import android.util.Log
import com.pedro.common.AudioCodec
import com.pedro.common.TimeUtils
import com.pedro.common.VideoCodec
import com.pedro.rtmp.amf.v0.*
import com.pedro.rtmp.flv.FlvPacket
import com.pedro.rtmp.rtmp.message.*
import com.pedro.rtmp.rtmp.message.control.Event
import com.pedro.rtmp.rtmp.message.control.Type
import com.pedro.rtmp.rtmp.message.control.UserControl
import com.pedro.rtmp.utils.CommandSessionHistory
import com.pedro.rtmp.utils.RtmpConfig
import com.pedro.rtmp.utils.socket.RtmpSocket
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.io.*

/**
 * Created by pedro on 21/04/21.
 */
abstract class CommandsManager {

  protected val TAG = "CommandsManager"

  val sessionHistory = CommandSessionHistory()
  var timestamp = 0
  protected var commandId = 0
  var streamId = 0
  var host = ""
  var port = 1935
  var appName = ""
  var streamName = ""
  var tcUrl = ""
  var user: String? = null
  var password: String? = null
  var onAuth = false
  var incrementalTs = false
  var startTs = 0L
  // VERSUS a/v-sync fix: rebase each track's FLV timestamp so audio and video BOTH start at 0.
  // RootEncoder stamps the first audio packet ~200ms before the first video packet (encoder-startup
  // latency, stamped as wall-clock PTS). Cloudflare Stream's transcoder collapses the recording to a
  // fraction of wall-clock unless both tracks start near 0. -1 = uninitialised; captured on the first
  // packet of each track, subtracted from every packet; cleared in reset().
  private var videoBaseTs = -1L
  private var audioBaseTs = -1L
  // VERSUS avsync2: last emitted (rebased) video FLV timestamp, to force the video timeline strictly
  // monotonic. MediaCodec occasionally stamps a frame a few ms before its predecessor; Cloudflare's
  // live packager assumes forward-only PTS and fails to build the recording ("missing or invalid
  // data") when back-jumps cluster. -1 = uninitialised; cleared in reset().
  private var lastVideoTs = -1L
  var readChunkSize = RtmpConfig.DEFAULT_CHUNK_SIZE
  var audioDisabled = false
  var videoDisabled = false
  private var bytesRead = 0
  private var acknowledgementSequence = 0

  protected var width = 640
  protected var height = 480
  var fps = 30
  protected var sampleRate = 44100
  protected var isStereo = true
  var videoCodec = VideoCodec.H264
  var audioCodec = AudioCodec.AAC
  //Avoid write a packet in middle of other.
  private val writeSync = Mutex(locked = false)

  fun setVideoResolution(width: Int, height: Int) {
    this.width = width
    this.height = height
  }

  fun setAudioInfo(sampleRate: Int, isStereo: Boolean) {
    this.sampleRate = sampleRate
    this.isStereo = isStereo
  }

  fun setAuth(user: String?, password: String?) {
    this.user = user
    this.password = password
  }

  protected fun getCurrentTimestamp(): Int {
    return (TimeUtils.getCurrentTimeMillis() / 1000 - timestamp).toInt()
  }

  @Throws(IOException::class)
  suspend fun sendChunkSize(socket: RtmpSocket) {
    writeSync.withLock {
      val output = socket.getOutStream()
      if (RtmpConfig.writeChunkSize != RtmpConfig.DEFAULT_CHUNK_SIZE) {
        val chunkSize = SetChunkSize(RtmpConfig.writeChunkSize)
        chunkSize.header.timeStamp = getCurrentTimestamp()
        chunkSize.header.messageStreamId = streamId
        chunkSize.writeHeader(output)
        chunkSize.writeBody(output)
        socket.flush()
        Log.i(TAG, "send $chunkSize")
      } else {
        Log.i(TAG, "using default write chunk size ${RtmpConfig.DEFAULT_CHUNK_SIZE}")
      }
    }
  }

  @Throws(IOException::class)
  suspend fun sendConnect(auth: String, socket: RtmpSocket) {
    writeSync.withLock {
      val output = socket.getOutStream()
      sendConnect(auth, output)
      socket.flush()
    }
  }

  @Throws(IOException::class)
  suspend fun createStream(socket: RtmpSocket) {
    writeSync.withLock {
      val output = socket.getOutStream()
      createStream(output)
      socket.flush()
    }
  }

  @Throws(IOException::class)
  fun readMessageResponse(socket: RtmpSocket): RtmpMessage {
    val input = socket.getInputStream()
    val message = RtmpMessage.getRtmpMessage(input, readChunkSize, sessionHistory)
    sessionHistory.setReadHeader(message.header)
    Log.i(TAG, "read $message")
    bytesRead += message.header.getPacketLength()
    return message
  }

  @Throws(IOException::class)
  suspend fun sendMetadata(socket: RtmpSocket) {
    writeSync.withLock {
      val output = socket.getOutStream()
      sendMetadata(output)
      socket.flush()
    }
  }

  @Throws(IOException::class)
  suspend fun sendPublish(socket: RtmpSocket) {
    writeSync.withLock {
      val output = socket.getOutStream()
      sendPublish(output)
      socket.flush()
    }
  }

  @Throws(IOException::class)
  suspend fun sendWindowAcknowledgementSize(socket: RtmpSocket) {
    writeSync.withLock {
      val output = socket.getOutStream()
      val windowAcknowledgementSize = WindowAcknowledgementSize(RtmpConfig.acknowledgementWindowSize, getCurrentTimestamp())
      windowAcknowledgementSize.writeHeader(output)
      windowAcknowledgementSize.writeBody(output)
      socket.flush()
    }
  }

  suspend fun sendPong(event: Event, socket: RtmpSocket) {
    writeSync.withLock {
      val output = socket.getOutStream()
      val pong = UserControl(Type.PONG_REPLY, event)
      pong.writeHeader(output)
      pong.writeBody(output)
      socket.flush()
      Log.i(TAG, "send pong")
    }
  }

  @Throws(IOException::class)
  suspend fun sendClose(socket: RtmpSocket) {
    writeSync.withLock {
      val output = socket.getOutStream()
      sendClose(output)
      socket.flush()
    }
  }

  suspend fun checkAndSendAcknowledgement(socket: RtmpSocket) {
    writeSync.withLock {
      if (bytesRead >= RtmpConfig.acknowledgementWindowSize) {
        acknowledgementSequence += bytesRead
        bytesRead -= RtmpConfig.acknowledgementWindowSize
        val output = socket.getOutStream()
        val acknowledgement = Acknowledgement(acknowledgementSequence)
        acknowledgement.writeHeader(output)
        acknowledgement.writeBody(output)
        output.flush()
        Log.i(TAG, "send $acknowledgement")
      }
    }
  }

  @Throws(IOException::class)
  suspend fun sendVideoPacket(flvPacket: FlvPacket, socket: RtmpSocket): Int {
    writeSync.withLock {
      val output = socket.getOutStream()
      if (incrementalTs) {
        flvPacket.timeStamp = ((TimeUtils.getCurrentTimeNano() / 1000 - startTs) / 1000)
      }
      // VERSUS: rebase video track so its first FLV timestamp is 0 (see videoBaseTs).
      if (videoBaseTs < 0) videoBaseTs = flvPacket.timeStamp
      flvPacket.timeStamp -= videoBaseTs
      // VERSUS avsync2: clamp the video timeline strictly monotonic (see lastVideoTs). A back-jumped
      // frame is pushed to lastVideoTs+1; the next in-order frame (larger ts) resets the ceiling, so
      // this does not accumulate drift. Video only — audio keeps its own rebase (avsync1 A/V offset).
      if (flvPacket.timeStamp <= lastVideoTs) flvPacket.timeStamp = lastVideoTs + 1
      lastVideoTs = flvPacket.timeStamp
      val video = Video(flvPacket, streamId)
      video.writeHeader(output)
      video.writeBody(output)
      socket.flush(true)
      return video.header.getPacketLength() //get packet size with header included to calculate bps
    }
  }

  @Throws(IOException::class)
  suspend fun sendAudioPacket(flvPacket: FlvPacket, socket: RtmpSocket): Int {
    writeSync.withLock {
      val output = socket.getOutStream()
      if (incrementalTs) {
        flvPacket.timeStamp = ((TimeUtils.getCurrentTimeNano() / 1000 - startTs) / 1000)
      }
      // VERSUS: rebase audio track so its first FLV timestamp is 0 (see audioBaseTs).
      if (audioBaseTs < 0) audioBaseTs = flvPacket.timeStamp
      flvPacket.timeStamp -= audioBaseTs
      val audio = Audio(flvPacket, streamId)
      audio.writeHeader(output)
      audio.writeBody(output)
      socket.flush(true)
      return audio.header.getPacketLength() //get packet size with header included to calculate bps
    }
  }

  abstract fun sendConnect(auth: String, output: OutputStream)
  abstract fun createStream(output: OutputStream)
  abstract fun sendMetadata(output: OutputStream)
  abstract fun sendPublish(output: OutputStream)
  abstract fun sendClose(output: OutputStream)

  fun reset() {
    startTs = 0
    videoBaseTs = -1L
    audioBaseTs = -1L
    lastVideoTs = -1L
    timestamp = 0
    streamId = 0
    commandId = 0
    readChunkSize = RtmpConfig.DEFAULT_CHUNK_SIZE
    sessionHistory.reset()
    acknowledgementSequence = 0
    bytesRead = 0
  }
}
