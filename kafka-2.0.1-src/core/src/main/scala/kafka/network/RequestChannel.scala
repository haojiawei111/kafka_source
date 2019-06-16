/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.network

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.concurrent._

import com.typesafe.scalalogging.Logger
import com.yammer.metrics.core.{Gauge, Meter}
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.{Logging, NotNothing, Pool}
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.network.Send
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.utils.{Sanitizer, Time}

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/**
  * 请求处理队列
  */
object RequestChannel extends Logging {
  private val requestLogger = Logger("kafka.request.logger")

  val RequestQueueSizeMetric = "RequestQueueSize"
  val ResponseQueueSizeMetric = "ResponseQueueSize"
  val ProcessorMetricTag = "processor"
  // log级别是否开启的Debug
  def isRequestLoggingEnabled: Boolean = requestLogger.underlying.isDebugEnabled


  // 监控
  class Metrics {

    private val metricsMap = mutable.Map[String, RequestMetrics]()

    (ApiKeys.values.toSeq.map(_.name) ++
        Seq(RequestMetrics.consumerFetchMetricName, RequestMetrics.followFetchMetricName)).foreach { name =>
      metricsMap.put(name, new RequestMetrics(name))
    }

    def apply(metricName: String) = metricsMap(metricName)

    def close(): Unit = {
       metricsMap.values.foreach(_.removeMetrics())
    }
  }

  //请求接口
  sealed trait BaseRequest
  // 关闭请求
  case object ShutdownRequest extends BaseRequest
  // 编码KafkaPrincipal.getName
  case class Session(principal: KafkaPrincipal, clientAddress: InetAddress) {
    val sanitizedUser = Sanitizer.sanitize(principal.getName)
  }

  class Request(val processor: Int,val context: RequestContext,val startTimeNanos: Long,memoryPool: MemoryPool,@volatile private var buffer: ByteBuffer,metrics: RequestChannel.Metrics) extends BaseRequest {
    // These need to be volatile because the readers are in the network thread and the writers are in the request
    // handler threads or the purgatory threads
    // 这些需要是易变的，因为读者在网络线程中，而编写者在请求
    // 处理程序线程或炼狱线程中
    @volatile var requestDequeueTimeNanos = -1L
    @volatile var apiLocalCompleteTimeNanos = -1L
    @volatile var responseCompleteTimeNanos = -1L
    @volatile var responseDequeueTimeNanos = -1L
    @volatile var apiRemoteCompleteTimeNanos = -1L
    @volatile var messageConversionsTimeNanos = 0L
    @volatile var temporaryMemoryBytes = 0L
    // 回调
    @volatile var recordNetworkThreadTimeCallback: Option[Long => Unit] = None

    val session = Session(context.principal, context.clientAddress)
    private val bodyAndSize: RequestAndSize = context.parseRequest(buffer)

    def sizeOfBodyInBytes: Int = bodyAndSize.size

    def header: RequestHeader = context.header
    //most request types are parsed entirely into objects at this point. for those we can release the underlying buffer.
    //some (like produce, or any time the schema contains fields of types BYTES or NULLABLE_BYTES) retain a reference
    //to the buffer. for those requests we cannot release the buffer early, but only when request processing is done.
    if (!header.apiKey.requiresDelayedAllocation) {
      releaseBuffer()
    }

    def requestDesc(details: Boolean): String = s"$header -- ${body[AbstractRequest].toString(details)}"

    def body[T <: AbstractRequest](implicit classTag: ClassTag[T], nn: NotNothing[T]): T = {
      bodyAndSize.request match {
        case r: T => r
        case r =>
          throw new ClassCastException(s"Expected request with type 带有类型的预期请求 ${classTag.runtimeClass}, but found ${r.getClass}")
      }
    }

    trace(s"Processor $processor received request: ${requestDesc(true)}")

    def requestThreadTimeNanos = {
      if (apiLocalCompleteTimeNanos == -1L) apiLocalCompleteTimeNanos = Time.SYSTEM.nanoseconds
      math.max(apiLocalCompleteTimeNanos - requestDequeueTimeNanos, 0L)
    }


    // 更新监控
    def updateRequestMetrics(networkThreadTimeNanos: Long, response: Response) {

      val endTimeNanos = Time.SYSTEM.nanoseconds
      // In some corner cases, apiLocalCompleteTimeNanos may not be set when the request completes if the remote
      // processing time is really small. This value is set in KafkaApis from a request handling thread.
      // This may be read in a network thread before the actual update happens in KafkaApis which will cause us to
      // see a negative value here. In that case, use responseCompleteTimeNanos as apiLocalCompleteTimeNanos.
      //api本地完成时间Nanos
      if (apiLocalCompleteTimeNanos < 0)
        apiLocalCompleteTimeNanos = responseCompleteTimeNanos //响应完成时间Nanos
      // If the apiRemoteCompleteTimeNanos is not set (i.e., for requests that do not go through a purgatory), then it is
      // the same as responseCompleteTimeNanos.
      //api远程完成时间Nanos
      if (apiRemoteCompleteTimeNanos < 0)
        apiRemoteCompleteTimeNanos = responseCompleteTimeNanos //响应完成时间Nanos

      /**
       * Converts nanos to millis with micros precision as additional decimal places in the request log have low
       * signal to noise ratio. When it comes to metrics, there is little difference either way as we round the value
       * to the nearest long.
        * 将微米精度的毫微转换为毫秒，因为请求日志中的附加小数位具有低信噪比。当谈到指标时，我们将值舍入到最接近的长度时，两者都没有什么区别。
       */
      def nanosToMs(nanos: Long): Double = {
        val positiveNanos = math.max(nanos, 0)
        TimeUnit.NANOSECONDS.toMicros(positiveNanos).toDouble / TimeUnit.MILLISECONDS.toMicros(1)
      }

      // 监控指标
      val requestQueueTimeMs = nanosToMs(requestDequeueTimeNanos - startTimeNanos)
      val apiLocalTimeMs = nanosToMs(apiLocalCompleteTimeNanos - requestDequeueTimeNanos)
      val apiRemoteTimeMs = nanosToMs(apiRemoteCompleteTimeNanos - apiLocalCompleteTimeNanos)
      val apiThrottleTimeMs = nanosToMs(responseCompleteTimeNanos - apiRemoteCompleteTimeNanos)
      val responseQueueTimeMs = nanosToMs(responseDequeueTimeNanos - responseCompleteTimeNanos)
      val responseSendTimeMs = nanosToMs(endTimeNanos - responseDequeueTimeNanos)
      val messageConversionsTimeMs = nanosToMs(messageConversionsTimeNanos)
      val totalTimeMs = nanosToMs(endTimeNanos - startTimeNanos)
      val fetchMetricNames =
        if (header.apiKey == ApiKeys.FETCH) {
          val isFromFollower = body[FetchRequest].isFromFollower
          Seq(
            if (isFromFollower) RequestMetrics.followFetchMetricName
            else RequestMetrics.consumerFetchMetricName
          )
        }
        else Seq.empty
      val metricNames = fetchMetricNames :+ header.apiKey.name
      metricNames.foreach { metricName =>
        val m = metrics(metricName)
        m.requestRate(header.apiVersion).mark()
        m.requestQueueTimeHist.update(Math.round(requestQueueTimeMs))
        m.localTimeHist.update(Math.round(apiLocalTimeMs))
        m.remoteTimeHist.update(Math.round(apiRemoteTimeMs))
        m.throttleTimeHist.update(Math.round(apiThrottleTimeMs))
        m.responseQueueTimeHist.update(Math.round(responseQueueTimeMs))
        m.responseSendTimeHist.update(Math.round(responseSendTimeMs))
        m.totalTimeHist.update(Math.round(totalTimeMs))
        m.requestBytesHist.update(sizeOfBodyInBytes)
        m.messageConversionsTimeHist.foreach(_.update(Math.round(messageConversionsTimeMs)))
        m.tempMemoryBytesHist.foreach(_.update(temporaryMemoryBytes))
      }

      // Records network handler thread usage. This is included towards the request quota for the
      // user/client. Throttling is only performed when request handler thread usage
      // is recorded, just before responses are queued for delivery.
      // The time recorded here is the time spent on the network thread for receiving this request
      // and sending the response. Note that for the first request on a connection, the time includes
      // the total time spent on authentication, which may be significant for SASL/SSL.
      //记录网络线程时间回调
      recordNetworkThreadTimeCallback.foreach(record => record(networkThreadTimeNanos))

      if (isRequestLoggingEnabled) {
        val detailsEnabled = requestLogger.underlying.isTraceEnabled
        val responseString = response.responseString.getOrElse(
          throw new IllegalStateException("responseAsString should always be defined if request logging is enabled"))
        val builder = new StringBuilder(256)
        builder.append("Completed request:").append(requestDesc(detailsEnabled))
          .append(",response:").append(responseString)
          .append(" from connection ").append(context.connectionId)
          .append(";totalTime:").append(totalTimeMs)
          .append(",requestQueueTime:").append(requestQueueTimeMs)
          .append(",localTime:").append(apiLocalTimeMs)
          .append(",remoteTime:").append(apiRemoteTimeMs)
          .append(",throttleTime:").append(apiThrottleTimeMs)
          .append(",responseQueueTime:").append(responseQueueTimeMs)
          .append(",sendTime:").append(responseSendTimeMs)
          .append(",securityProtocol:").append(context.securityProtocol)
          .append(",principal:").append(session.principal)
          .append(",listener:").append(context.listenerName.value)
        if (temporaryMemoryBytes > 0)
          builder.append(",temporaryMemoryBytes:").append(temporaryMemoryBytes)
        if (messageConversionsTimeMs > 0)
          builder.append(",messageConversionsTime:").append(messageConversionsTimeMs)
        requestLogger.debug(builder.toString)
      }
    }


    def releaseBuffer(): Unit = {
      if (buffer != null) {
        // 把内存放回内存池
        memoryPool.release(buffer)
        buffer = null
      }
    }

    override def toString = s"Request(processor=$processor, " +
      s"connectionId=${context.connectionId}, " +
      s"session=$session, " +
      s"listenerName=${context.listenerName}, " +
      s"securityProtocol=${context.securityProtocol}, " +
      s"buffer=$buffer)"

  }



  // 响应接口   内部静态类
  abstract class Response(val request: Request) {
    locally {
      val nowNs = Time.SYSTEM.nanoseconds
      request.responseCompleteTimeNanos = nowNs
      if (request.apiLocalCompleteTimeNanos == -1L)
        request.apiLocalCompleteTimeNanos = nowNs
    }
    // processor的ID
    def processor: Int = request.processor

    def responseString: Option[String] = Some("")

    def onComplete: Option[Send => Unit] = None

    override def toString: String
  }

  /** responseAsString should only be defined if request logging is enabled 只有在启用请求记录时才应定义responseAsString*/
  class SendResponse(request: Request,val responseSend: Send,val responseAsString: Option[String],val onCompleteCallback: Option[Send => Unit]) extends Response(request) {
    override def responseString: Option[String] = responseAsString

    override def onComplete: Option[Send => Unit] = onCompleteCallback

    override def toString: String =
      s"Response(type=Send, request=$request, send=$responseSend, asString=$responseAsString)"
  }
  //不用响应
  class NoOpResponse(request: Request) extends Response(request) {
    override def toString: String =
      s"Response(type=NoOp, request=$request)"
  }
  // 关闭连接的响应
  class CloseConnectionResponse(request: Request) extends Response(request) {
    override def toString: String =
      s"Response(type=CloseConnection, request=$request)"
  }
  //开始限制的响应
  class StartThrottlingResponse(request: Request) extends Response(request) {
    override def toString: String =
      s"Response(type=StartThrottling, request=$request)"
  }
  // 结束限制的响应
  class EndThrottlingResponse(request: Request) extends Response(request) {
    override def toString: String =
      s"Response(type=EndThrottling, request=$request)"
  }
}


class RequestChannel(val queueSize: Int) extends KafkaMetricsGroup {
  import RequestChannel._
  // 创建RequestChannel的内部静态类Metrics
  val metrics = new RequestChannel.Metrics

  // 请求队列
  private val requestQueue = new ArrayBlockingQueue[BaseRequest](queueSize)

  // 线程安全的map，SockstServer中创建的所有Processor都要交给这里管理
  private val processors = new ConcurrentHashMap[Int, Processor]()

  newGauge(RequestQueueSizeMetric, new Gauge[Int] {
      def value = requestQueue.size
  })

  newGauge(ResponseQueueSizeMetric, new Gauge[Int]{
    def value = processors.values.asScala.foldLeft(0) {(total, processor) =>
      total + processor.responseQueueSize
    }
  })


  def addProcessor(processor: Processor): Unit = {
    //putIfAbsent   如果传入key对应的value已经存在，就返回存在的value，不进行替换。如果不存在，就添加key和value，返回null
    if (processors.putIfAbsent(processor.id, processor) != null)
      warn(s"Unexpected processor with processorId带有processorId的意外处理器 ${processor.id}")

    newGauge(ResponseQueueSizeMetric,
      new Gauge[Int] {
        def value = processor.responseQueueSize
      },
      Map(ProcessorMetricTag -> processor.id.toString)
    )
  }

  def removeProcessor(processorId: Int): Unit = {
    processors.remove(processorId)
    // 移除监控
    removeMetric(ResponseQueueSizeMetric, Map(ProcessorMetricTag -> processorId.toString))
  }

  /** 发送请求处理，可能阻塞，直到请求队列中有空间*/
  def sendRequest(request: RequestChannel.Request) {
    requestQueue.put(request)
  }

  /**  将响应发送回要通过网络发送的套接字服务器*/
  def sendResponse(response: RequestChannel.Response) {
    // 如果日志级别为Trace，这执行这个if
    if (isTraceEnabled) {
      val requestHeader = response.request.header
      val message = response match {
        case sendResponse: SendResponse =>
          s"Sending ${requestHeader.apiKey} response to client ${requestHeader.clientId} of ${sendResponse.responseSend.size} bytes."
        case _: NoOpResponse =>
          s"Not sending ${requestHeader.apiKey} response to client ${requestHeader.clientId} as it's not required."
        case _: CloseConnectionResponse =>
          s"Closing connection for client ${requestHeader.clientId} due to error during ${requestHeader.apiKey}."
        case _: StartThrottlingResponse =>
          s"Notifying channel throttling has started for client ${requestHeader.clientId} for ${requestHeader.apiKey}"
        case _: EndThrottlingResponse =>
          s"Notifying channel throttling has ended for client ${requestHeader.clientId} for ${requestHeader.apiKey}"
      }
      trace(message)
    }

    val processor = processors.get(response.processor)
    // The processor may be null if it was shutdown. In this case, the connections
    // are closed, so the response is dropped.
    // 如果处理器关闭，处理器可能为空。在这种情况下，连接被关闭，因此响应被删除。
    if (processor != null) {
      processor.enqueueResponse(response)
    }
  }

  /** 获取下一个请求或阻止，直到指定的时间过去 */
  def receiveRequest(timeout: Long): RequestChannel.BaseRequest = requestQueue.poll(timeout, TimeUnit.MILLISECONDS)

  /** 获取下一个请求或阻止，直到有一个请求或阻止 */
  def receiveRequest(): RequestChannel.BaseRequest =
    requestQueue.take()

  def updateErrorMetrics(apiKey: ApiKeys, errors: collection.Map[Errors, Integer]) {
    errors.foreach { case (error, count) =>
      metrics(apiKey.name).markErrorMeter(error, count)
    }
  }
  // 清理请求队列
  def clear() {
    requestQueue.clear()
  }
  // 清理请求队列并关闭监控
  def shutdown() {
    clear()
    metrics.close()
  }
  // 往请求队列中添加关闭请求
  def sendShutdownRequest(): Unit = requestQueue.put(ShutdownRequest)

}







// Request监控
object RequestMetrics {
  val consumerFetchMetricName = ApiKeys.FETCH.name + "Consumer"
  val followFetchMetricName = ApiKeys.FETCH.name + "Follower"

  val RequestsPerSec = "RequestsPerSec"
  val RequestQueueTimeMs = "RequestQueueTimeMs"
  val LocalTimeMs = "LocalTimeMs"
  val RemoteTimeMs = "RemoteTimeMs"
  val ThrottleTimeMs = "ThrottleTimeMs"
  val ResponseQueueTimeMs = "ResponseQueueTimeMs"
  val ResponseSendTimeMs = "ResponseSendTimeMs"
  val TotalTimeMs = "TotalTimeMs"
  val RequestBytes = "RequestBytes"
  val MessageConversionsTimeMs = "MessageConversionsTimeMs"
  val TemporaryMemoryBytes = "TemporaryMemoryBytes"
  val ErrorsPerSec = "ErrorsPerSec"
}

class RequestMetrics(name: String) extends KafkaMetricsGroup {

  import RequestMetrics._

  val tags = Map("request" -> name)
  val requestRateInternal = new Pool[Short, Meter]()
  // time a request spent in a request queue
  val requestQueueTimeHist = newHistogram(RequestQueueTimeMs, biased = true, tags)
  // time a request takes to be processed at the local broker
  val localTimeHist = newHistogram(LocalTimeMs, biased = true, tags)
  // time a request takes to wait on remote brokers (currently only relevant to fetch and produce requests)
  val remoteTimeHist = newHistogram(RemoteTimeMs, biased = true, tags)
  // time a request is throttled
  val throttleTimeHist = newHistogram(ThrottleTimeMs, biased = true, tags)
  // time a response spent in a response queue
  val responseQueueTimeHist = newHistogram(ResponseQueueTimeMs, biased = true, tags)
  // time to send the response to the requester
  val responseSendTimeHist = newHistogram(ResponseSendTimeMs, biased = true, tags)
  val totalTimeHist = newHistogram(TotalTimeMs, biased = true, tags)
  // request size in bytes
  val requestBytesHist = newHistogram(RequestBytes, biased = true, tags)
  // time for message conversions (only relevant to fetch and produce requests)
  val messageConversionsTimeHist =
    if (name == ApiKeys.FETCH.name || name == ApiKeys.PRODUCE.name)
      Some(newHistogram(MessageConversionsTimeMs, biased = true, tags))
    else
      None
  // Temporary memory allocated for processing request (only populated for fetch and produce requests)
  // This shows the memory allocated for compression/conversions excluding the actual request size
  val tempMemoryBytesHist =
    if (name == ApiKeys.FETCH.name || name == ApiKeys.PRODUCE.name)
      Some(newHistogram(TemporaryMemoryBytes, biased = true, tags))
    else
      None

  private val errorMeters = mutable.Map[Errors, ErrorMeter]()
  Errors.values.foreach(error => errorMeters.put(error, new ErrorMeter(name, error)))

  def requestRate(version: Short): Meter = {
    requestRateInternal.getAndMaybePut(version, newMeter("RequestsPerSec", "requests", TimeUnit.SECONDS, tags + ("version" -> version.toString)))
  }

  class ErrorMeter(name: String, error: Errors) {
    private val tags = Map("request" -> name, "error" -> error.name)

    @volatile private var meter: Meter = null

    def getOrCreateMeter(): Meter = {
      if (meter != null)
        meter
      else {
        synchronized {
          if (meter == null)
             meter = newMeter(ErrorsPerSec, "requests", TimeUnit.SECONDS, tags)
          meter
        }
      }
    }

    def removeMeter(): Unit = {
      synchronized {
        if (meter != null) {
          removeMetric(ErrorsPerSec, tags)
          meter = null
        }
      }
    }
  }

  def markErrorMeter(error: Errors, count: Int) {
    errorMeters(error).getOrCreateMeter().mark(count.toLong)
  }

  def removeMetrics(): Unit = {
    for (version <- requestRateInternal.keys) removeMetric(RequestsPerSec, tags + ("version" -> version.toString))
    removeMetric(RequestQueueTimeMs, tags)
    removeMetric(LocalTimeMs, tags)
    removeMetric(RemoteTimeMs, tags)
    removeMetric(RequestsPerSec, tags)
    removeMetric(ThrottleTimeMs, tags)
    removeMetric(ResponseQueueTimeMs, tags)
    removeMetric(TotalTimeMs, tags)
    removeMetric(ResponseSendTimeMs, tags)
    removeMetric(RequestBytes, tags)
    removeMetric(ResponseSendTimeMs, tags)
    if (name == ApiKeys.FETCH.name || name == ApiKeys.PRODUCE.name) {
      removeMetric(MessageConversionsTimeMs, tags)
      removeMetric(TemporaryMemoryBytes, tags)
    }
    errorMeters.values.foreach(_.removeMeter())
    errorMeters.clear()
  }
}
