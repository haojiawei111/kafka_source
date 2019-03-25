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

import java.io.IOException
import java.net._
import java.nio.channels._
import java.nio.channels.{Selector => NSelector}
import java.util.concurrent._
import java.util.concurrent.atomic._

import com.yammer.metrics.core.Gauge
import kafka.cluster.{BrokerEndPoint, EndPoint}
import kafka.metrics.KafkaMetricsGroup
import kafka.network.RequestChannel.{CloseConnectionResponse, EndThrottlingResponse, NoOpResponse, SendResponse, StartThrottlingResponse}
import kafka.security.CredentialProvider
import kafka.server.KafkaConfig
import kafka.utils._
import org.apache.kafka.common.{KafkaException, Reconfigurable}
import org.apache.kafka.common.memory.{MemoryPool, SimpleMemoryPool}
import org.apache.kafka.common.metrics._
import org.apache.kafka.common.metrics.stats.Meter
import org.apache.kafka.common.network.KafkaChannel.ChannelMuteEvent
import org.apache.kafka.common.network.{ChannelBuilder, ChannelBuilders, KafkaChannel, ListenerName, Selectable, Send, Selector => KSelector}
import org.apache.kafka.common.requests.{RequestContext, RequestHeader}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.{KafkaThread, LogContext, Time}
import org.slf4j.event.Level

import scala.collection._
import JavaConverters._
import scala.collection.mutable.{ArrayBuffer, Buffer}
import scala.util.control.ControlThrowable

/**
 * An NIO socket server. The threading model is
 *   1 Acceptor thread that handles new connections
 *   Acceptor has N Processor threads that each have their own selector and read requests from sockets
 *   M Handler threads that handle requests and produce responses back to the processor threads for writing.
  *   处理新连接的1接受器线程
  *   接收器有N个处理器线程，每个线程都有自己的选择器和来自套接字的读取请求。
  *   M Handler线程处理请求并生成响应到处理器线程进行写入。
 */
class SocketServer(val config: KafkaConfig, val metrics: Metrics, val time: Time, val credentialProvider: CredentialProvider) extends Logging with KafkaMetricsGroup {

  // queued.max.requests 最大请求管道数
  private val maxQueuedRequests = config.queuedMaxRequests

  // max.connections.per.ip
  private val maxConnectionsPerIp = config.maxConnectionsPerIp
  // max.connections.per.ip.overrides
  private val maxConnectionsPerIpOverrides = config.maxConnectionsPerIpOverrides

  private val logContext = new LogContext(s"[SocketServer brokerId=${config.brokerId}] ")
  this.logIdent = logContext.logPrefix

  // 内存池监控
  private val memoryPoolSensor = metrics.sensor("MemoryPoolUtilization")
  private val memoryPoolDepletedPercentMetricName = metrics.metricName("MemoryPoolAvgDepletedPercent", "socket-server-metrics")
  private val memoryPoolDepletedTimeMetricName = metrics.metricName("MemoryPoolDepletedTimeTotal", "socket-server-metrics")
  memoryPoolSensor.add(new Meter(TimeUnit.MILLISECONDS, memoryPoolDepletedPercentMetricName, memoryPoolDepletedTimeMetricName))


  // 创建内存池
  private val memoryPool = if (config.queuedMaxBytes > 0) new SimpleMemoryPool(config.queuedMaxBytes, config.socketRequestMaxBytes, false, memoryPoolSensor) else MemoryPool.NONE

  // 创建RequestChannel请求处理管道
  val requestChannel = new RequestChannel(maxQueuedRequests)
  // int和Processor的映射
  private val processors = new ConcurrentHashMap[Int, Processor]()
  // EndPoint和Acceptor的映射
  private[network] val acceptors = new ConcurrentHashMap[EndPoint, Acceptor]()

  // Processor全局的ID号
  private var nextProcessorId = 0

  // 在startup方法中个实例化connectionQuotas = new ConnectionQuotas(maxConnectionsPerIp, maxConnectionsPerIpOverrides)
  private var connectionQuotas: ConnectionQuotas = _

  private var stoppedProcessingRequests = false

  /**
   * Start the socket server. Acceptors for all the listeners are started. Processors
   * are started if `startupProcessors` is true. If not, processors are only started when
   * [[kafka.network.SocketServer#startProcessors()]] is invoked. Delayed starting of processors
   * is used to delay processing client connections until server is fully initialized, e.g.
   * to ensure that all credentials have been loaded before authentications are performed.
   * Acceptors are always started during `startup` so that the bound port is known when this
   * method completes even when ephemeral ports are used. Incoming connections on this server
   * are processed when processors start up and invoke [[org.apache.kafka.common.network.Selector#poll]].
   *
   * @param startupProcessors Flag indicating whether `Processor`s must be started.
   */
  def startup(startupProcessors: Boolean = true) {
    this.synchronized {
      connectionQuotas = new ConnectionQuotas(maxConnectionsPerIp, maxConnectionsPerIpOverrides)
      //根据配置的若干endpoint创建相应的Acceptor及相关联的一组Processor线程

      createAcceptorAndProcessors(config.numNetworkThreads, config.listeners)

      if (startupProcessors) {
        startProcessors()
      }
    }

    newGauge("NetworkProcessorAvgIdlePercent",
      new Gauge[Double] {

        def value = SocketServer.this.synchronized {
          val ioWaitRatioMetricNames = processors.values.asScala.map { p =>
            metrics.metricName("io-wait-ratio", "socket-server-metrics", p.metricTags)
          }
          ioWaitRatioMetricNames.map { metricName =>
            Option(metrics.metric(metricName)).fold(0.0)(m => Math.min(m.metricValue.asInstanceOf[Double], 1.0))
          }.sum / processors.size
        }
      }
    )
    newGauge("MemoryPoolAvailable",
      new Gauge[Long] {
        def value = memoryPool.availableMemory()
      }
    )
    newGauge("MemoryPoolUsed",
      new Gauge[Long] {
        def value = memoryPool.size() - memoryPool.availableMemory()
      }
    )
    info("Started " + acceptors.size + " acceptor threads")
  }

  /**
   * Starts processors of all the acceptors of this server if they have not already been started.
   * This method is used for delayed starting of processors if [[kafka.network.SocketServer#startup]]
   * was invoked with `startupProcessors=false`.
    * 启动此服务器的所有接收器的处理器（如果尚未启动）。
    * 如果使用`startupProcessors = false`调用[[kafka.network.SocketServer＃startup]] *，则此方法用于延迟启动处理器
   */
  def startProcessors(): Unit = synchronized {
    // 开始Processor线程
    acceptors.values.asScala.foreach { _.startProcessors() }
    info(s"Started processors for ${acceptors.size} acceptors")
  }

  private def endpoints = config.listeners.map(l => l.listenerName -> l).toMap

  // 创建Acceptor，有几个endpoints就常见几个Acceptor，那个Acceptor根据num.network.threads的配置生成一组Processor线程
  private def createAcceptorAndProcessors(processorsPerListener: Int,endpoints: Seq[EndPoint]): Unit = synchronized {

    val sendBufferSize = config.socketSendBufferBytes
    val recvBufferSize = config.socketReceiveBufferBytes
    val brokerId = config.brokerId
    //根据配置的若干endpoint创建相应的Acceptor及相关联的一组Processor线程
    endpoints.foreach { endpoint =>
      val listenerName = endpoint.listenerName
      val securityProtocol = endpoint.securityProtocol

      val acceptor = new Acceptor(endpoint, sendBufferSize, recvBufferSize, brokerId, connectionQuotas)
      addProcessors(acceptor, endpoint, processorsPerListener)
      //启动Acceptor线程
      KafkaThread.nonDaemon(s"kafka-socket-acceptor-$listenerName-$securityProtocol-${endpoint.port}", acceptor).start()
      acceptor.awaitStartup()
      acceptors.put(endpoint, acceptor)
    }
  }

  private def addProcessors(acceptor: Acceptor, endpoint: EndPoint, newProcessorsPerListener: Int): Unit = synchronized {
    val listenerName = endpoint.listenerName
    val securityProtocol = endpoint.securityProtocol

    val listenerProcessors = new ArrayBuffer[Processor]()

    // 这里每个Acceptor会生成一组Processor，配置文件中配置这个num.network.threads
    for (_ <- 0 until newProcessorsPerListener) {
      val processor = newProcessor(nextProcessorId, connectionQuotas, listenerName, securityProtocol, memoryPool)
      listenerProcessors += processor
      //把processor添加到requestChannel
      requestChannel.addProcessor(processor)
      // Processor全局ID
      nextProcessorId += 1
    }

    listenerProcessors.foreach(p => processors.put(p.id, p))
    // 这个把acceptor的Processor数量添加到acceptor
    acceptor.addProcessors(listenerProcessors)
  }

  /**
    * 停止处理请求和新连接。
    */
  def stopProcessingRequests() = {
    info("Stopping socket server request processors")
    this.synchronized {
      acceptors.asScala.values.foreach(_.shutdown())
      processors.asScala.values.foreach(_.shutdown())
      requestChannel.clear()
      stoppedProcessingRequests = true
    }
    info("Stopped socket server request processors")
  }


  // 调整线程池  Processors，配置文件里面配置的有多少线程这里有会创建几个Processor线程
  def resizeThreadPool(oldNumNetworkThreads: Int, newNumNetworkThreads: Int): Unit = synchronized {
    info(s"Resizing network thread pool size for each listener from $oldNumNetworkThreads to $newNumNetworkThreads")
    if (newNumNetworkThreads > oldNumNetworkThreads) { //线程池数据增加
      acceptors.asScala.foreach { case (endpoint, acceptor) =>
        addProcessors(acceptor, endpoint, newNumNetworkThreads - oldNumNetworkThreads)
      }
    } else if (newNumNetworkThreads < oldNumNetworkThreads) //线程池数据减少
      acceptors.asScala.values.foreach(_.removeProcessors(oldNumNetworkThreads - newNumNetworkThreads, requestChannel))
  }

  /**
    * Shutdown the socket server. If still processing requests, shutdown
    * acceptors and processors first.
    */
  def shutdown() = {
    info("Shutting down socket server")
    this.synchronized {
      if (!stoppedProcessingRequests)
        stopProcessingRequests()
      requestChannel.shutdown()
    }
    info("Shutdown completed")
  }

  def boundPort(listenerName: ListenerName): Int = {
    try {
//      假设我的tomcat服务器端口是80，对外端口是通过防火墙设置后映射出来的，为8050，
//      用getLocalhost获取到的是80端口，
//      用getServerPort获取到的是8050端口。
//      我的理解是：
//      getLocalhost获取的是最后的端口，无论中间经过多少端口转发；
//      getServerPor获取的是当前访问的链接端口。假设我们的链接是http://localhost:8090/AA，那么获取的端口为8090。
      acceptors.get(endpoints(listenerName)).serverChannel.socket.getLocalPort
    } catch {
      case e: Exception =>
        throw new KafkaException("Tried to check server's port before server was started or checked for port of non-existing protocol试图在服务器启动之前检查服务器的端口或检查不存在的协议的端口", e)
    }
  }

  def addListeners(listenersAdded: Seq[EndPoint]): Unit = synchronized {
    info(s"Adding listeners for endpoints $listenersAdded")
    createAcceptorAndProcessors(config.numNetworkThreads, listenersAdded)
    startProcessors()
  }

  def removeListeners(listenersRemoved: Seq[EndPoint]): Unit = synchronized {
    info(s"Removing listeners for endpoints删除端口的侦听器 $listenersRemoved")
    listenersRemoved.foreach { endpoint =>
      acceptors.asScala.remove(endpoint).foreach(_.shutdown())
    }
  }

  /* `protected` for test usage `protected`用于测试用法 */
  protected[network] def newProcessor(id: Int, connectionQuotas: ConnectionQuotas, listenerName: ListenerName,securityProtocol: SecurityProtocol, memoryPool: MemoryPool): Processor = {
    new Processor(id,
      time,
      config.socketRequestMaxBytes,
      requestChannel,
      connectionQuotas,
      config.connectionsMaxIdleMs,
      listenerName,
      securityProtocol,
      config,
      metrics,
      credentialProvider,
      memoryPool,
      logContext
    )
  }

  /* For test usage 用于测试 返回address的限制信息*/
  private[network] def connectionCount(address: InetAddress): Int =
    Option(connectionQuotas).fold(0)(_.get(address))

  /* For test usage  用于测试 从processors中返回指定index的processor*/
  private[network] def processor(index: Int): Processor = processors.get(index)

}




/**
  * 一个带有一些辅助变量和方法的基类   AbstractServerThread实现类：Acceptor
 */
private[kafka] abstract class AbstractServerThread(connectionQuotas: ConnectionQuotas) extends Runnable with Logging {

  private val startupLatch = new CountDownLatch(1)

  // `shutdown()` is invoked before `startupComplete` and `shutdownComplete` if an exception is thrown in the constructor
  // (e.g. if the address is already in use). We want `shutdown` to proceed in such cases, so we first assign an open
  // latch and then replace it in `startupComplete()`.
  @volatile private var shutdownLatch = new CountDownLatch(0)

  private val alive = new AtomicBoolean(true)

  def wakeup(): Unit

  /**
   * Initiates a graceful shutdown by signaling to stop and waiting for the shutdown to complete
   */
  def shutdown(): Unit = {
    if (alive.getAndSet(false))
      wakeup()
    shutdownLatch.await()
  }

  /**
   * Wait for the thread to completely start up
   */
  def awaitStartup(): Unit = startupLatch.await

  /**
   * Record that the thread startup is complete
    * 记录线程启动完成
   */
  protected def startupComplete(): Unit = {
    // Replace the open latch with a closed one
    shutdownLatch = new CountDownLatch(1)
    startupLatch.countDown()
  }

  /**
   * Record that the thread shutdown is complete
    * 记录线程关闭完成
   */
  protected def shutdownComplete(): Unit = shutdownLatch.countDown()

  /**
   * Is the server still running?
   */
  protected def isRunning: Boolean = alive.get

  /**
   * Close `channel` and decrement the connection count.
   */
  def close(channel: SocketChannel): Unit = {
    if (channel != null) {
      debug("Closing connection from " + channel.socket.getRemoteSocketAddress())
      connectionQuotas.dec(channel.socket.getInetAddress)
      CoreUtils.swallow(channel.socket().close(), this, Level.ERROR)
      CoreUtils.swallow(channel.close(), this, Level.ERROR)
    }
  }
}

/**
  * Kafka网络层头号马仔:Acceptor类
  * Acceptor 作两件事: 创建一堆worker线程；接受新连接, 将新的socket指派给某个 worker线程;
 * Thread that accepts and configures new connections. There is one of these per endpoint.
 */
private[kafka] class Acceptor(val endPoint: EndPoint,val sendBufferSize: Int,val recvBufferSize: Int,brokerId: Int,connectionQuotas: ConnectionQuotas) extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {

  private val nioSelector = NSelector.open()

  //创建监听ServerSocket:  ServerSocketChannel服务端套接字通道
  val serverChannel : ServerSocketChannel = openServerSocket(endPoint.host, endPoint.port)

  // Acceptor中持有的processor线程
  private val processors = new ArrayBuffer[Processor]()
  // 启动的processors
  private val processorsStarted = new AtomicBoolean

  // 把Processor添加到processors数组中
  private[network] def addProcessors(newProcessors: Buffer[Processor]): Unit = synchronized {
    processors ++= newProcessors
    // 如果processorsStarted是true，则只启动newProcessors
    if (processorsStarted.get)
      startProcessors(newProcessors)
  }

  private[network] def startProcessors(): Unit = synchronized {
    //如果processorsStarted是false，第一次启动，把如果processorsStarted置为true并启动processors
    if (!processorsStarted.getAndSet(true)) {
      startProcessors(processors)
    }
  }

  // 启动Processors数组的Processor
  private def startProcessors(processors: Seq[Processor]): Unit = synchronized {
    processors.foreach { processor =>
      KafkaThread.nonDaemon(s"kafka-network-thread-$brokerId-${endPoint.listenerName}-${endPoint.securityProtocol}-${processor.id}",
        processor).start()
    }
  }
  //移除Processor  1.移除Acceptor的processor 2. 关闭processor   3.关闭requestChannel里的processor
  private[network] def removeProcessors(removeCount: Int, requestChannel: RequestChannel): Unit = synchronized {
    // Shutdown `removeCount` processors. Remove them from the processor list first so that no more
    // connections are assigned. Shutdown the removed processors, closing the selector and its connections.
    // The processors are then removed from `requestChannel` and any pending responses to these processors are dropped.
    // 关闭`removeCount`处理器。首先从处理器列表中删除它们，以便不再分配
    // 连接。关闭已删除的处理器，关闭选择器及其连接。
    // 然后从`requestChannel`中删除处理器，并删除对这些处理器的任何挂起响应。

    //    take(3)---> 表示，取出前3个元素
    //    takeRight(3)----> 表示，从后面/右边开始获取，取出3个元素
    //    takeWhile()---> 表示，从左边开始，将满足条件的元素取出来，直到遇到第一个不满足条件的元素
    val toRemove = processors.takeRight(removeCount)
    processors.remove(processors.size - removeCount, removeCount)

    toRemove.foreach(_.shutdown())
    toRemove.foreach(processor => requestChannel.removeProcessor(processor.id))
  }

  // 关闭Acceptor和processors
  override def shutdown(): Unit = {
    super.shutdown()
    synchronized {
      processors.foreach(_.shutdown())
    }
  }

  /**
   * Accept loop that checks for new connection attempts
    * 接受检查新连接尝试的循环   Acceptor工作的主要逻辑
   */
  def run() {
    //注册Channel到Selector上(Registering Channels with the Selector)
    serverChannel.register(nioSelector, SelectionKey.OP_ACCEPT)
    //记录线程启动完成
    startupComplete()

    try {

      var currentProcessor = 0

      while (isRunning) {
        try {
          // select方法会返回所有处于就绪状态的channel数量 select()方法在返回channel之前处于阻塞状态。 select(long timeout)和select做的事一样，不过他的阻塞有一个超时限制。
          val ready = nioSelector.select(500)
          if (ready > 0) {
            //在调用select并返回了有channel就绪之后，可以通过选中的key集合来获取channel
            val keys = nioSelector.selectedKeys()
            val iter = keys.iterator()
            while (iter.hasNext && isRunning) {
              try {
                val key = iter.next
                iter.remove()
                if (key.isAcceptable) {
                  // ServerSocketChannel接受了连接。
                  val processor = synchronized {
                    // 轮询processors，平均发给processor线程
                    currentProcessor = currentProcessor % processors.size
                    processors(currentProcessor)
                  }
                  accept(key, processor)
                } else
                  throw new IllegalStateException("Unrecognized key state for acceptor thread.接受者线程无法识别的键状态。")


                // 循环到下一个处理器线程，mod（numProcessors）将在稍后完成
                currentProcessor = currentProcessor + 1
              } catch {
                case e: Throwable => error("Error while accepting connection", e)
              }
            }
          }
        }
        catch {
          // We catch all the throwables to prevent the acceptor thread from exiting on exceptions due
          // to a select operation on a specific channel or a bad request. We don't want
          // the broker to stop responding to requests from other clients in these scenarios.
          case e: ControlThrowable => throw e
          case e: Throwable => error("Error occurred", e)
        }

      }
    }
    finally {
      debug("Closing server socket and selector.")
      CoreUtils.swallow(serverChannel.close(), this, Level.ERROR)
      CoreUtils.swallow(nioSelector.close(), this, Level.ERROR)
      shutdownComplete()
    }
  }

  /*
   * 创建服务器套接字以侦听连接。
   */
  private def openServerSocket(host: String, port: Int): ServerSocketChannel = {

    val socketAddress =
      if (host == null || host.trim.isEmpty)
        new InetSocketAddress(port)
      else
        new InetSocketAddress(host, port)

    // 获得一个ServerSocket通道
    val serverChannel = ServerSocketChannel.open()
    // 设置通道为非阻塞
    serverChannel.configureBlocking(false)

    if (recvBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
      serverChannel.socket().setReceiveBufferSize(recvBufferSize)

    try {
      // 将该通道对于的serverSocket绑定到port端口，比如8989
      serverChannel.socket.bind(socketAddress)
      info("Awaiting socket connections on %s:%d.".format(socketAddress.getHostString, serverChannel.socket.getLocalPort))
    } catch {
      case e: SocketException =>
        throw new KafkaException("Socket server failed to bind to %s:%d: %s.".format(socketAddress.getHostString, port, e.getMessage), e)
    }
    serverChannel
  }

  /*
   * 把从ServerSocketChannel中获取的SelectionKey，然后给了processor处理
   */
  def accept(key: SelectionKey, processor: Processor) {
    // 拿到key对应的serverSocketChannel
    val serverSocketChannel = key.channel().asInstanceOf[ServerSocketChannel]
    val socketChannel : SocketChannel = serverSocketChannel.accept()
    try {
      // 一个SockstServer就一个connectionQuotas，这个ip增加一个连接
      connectionQuotas.inc(socketChannel.socket().getInetAddress)
      socketChannel.configureBlocking(false)
      socketChannel.socket().setTcpNoDelay(true)
      socketChannel.socket().setKeepAlive(true)

      if (sendBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
        socketChannel.socket().setSendBufferSize(sendBufferSize)

      debug("Accepted connection from %s on %s and assigned it to processor %d, sendBufferSize [actual|requested]: [%d|%d] recvBufferSize [actual|requested]: [%d|%d]"
            .format(socketChannel.socket.getRemoteSocketAddress, socketChannel.socket.getLocalSocketAddress, processor.id,
                  socketChannel.socket.getSendBufferSize, sendBufferSize,
                  socketChannel.socket.getReceiveBufferSize, recvBufferSize))

      processor.accept(socketChannel)

    } catch {
      case e: TooManyConnectionsException =>
        info("Rejected connection from %s, address already has the configured maximum of %d connections.".format(e.ip, e.count))
        close(socketChannel)
    }
  }

  /**
   * Wakeup the thread for selection.
    * 唤醒线程进行选择。
   */
  @Override
  def wakeup = nioSelector.wakeup()

}




private[kafka] object Processor {
  val IdlePercentMetricName = "IdlePercent"
  val NetworkProcessorMetricTag = "networkProcessor"
  val ListenerMetricTag = "listener"
}

/**
 * Thread that processes all requests from a single connection. There are N of these running in parallel
 * each of which has its own selector
 */
private[kafka] class Processor(val id: Int,time: Time,maxRequestSize: Int,requestChannel: RequestChannel,connectionQuotas: ConnectionQuotas,connectionsMaxIdleMs: Long,listenerName: ListenerName,securityProtocol: SecurityProtocol,
                               config: KafkaConfig,metrics: Metrics,credentialProvider: CredentialProvider,memoryPool: MemoryPool,logContext: LogContext) extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {

  import Processor._

  private object ConnectionId {
    def fromString(s: String): Option[ConnectionId] = s.split("-") match {
      case Array(local, remote, index) => BrokerEndPoint.parseHostPort(local).flatMap { case (localHost, localPort) =>
        BrokerEndPoint.parseHostPort(remote).map { case (remoteHost, remotePort) =>
          ConnectionId(localHost, localPort, remoteHost, remotePort, Integer.parseInt(index))
        }
      }
      case _ => None
    }
  }

  private[network] case class ConnectionId(localHost: String, localPort: Int, remoteHost: String, remotePort: Int, index: Int) {
    override def toString: String = s"$localHost:$localPort-$remoteHost:$remotePort-$index"
  }
  // 核心的队列
  //连接队列
  private val newConnections = new ConcurrentLinkedQueue[SocketChannel]()
  //存响应的map
  private val inflightResponses = mutable.Map[String, RequestChannel.Response]()
  //存响应的阻塞队列（
  private val responseQueue = new LinkedBlockingDeque[RequestChannel.Response]()

  // 监控的标签
  private[kafka] val metricTags = mutable.LinkedHashMap(
    ListenerMetricTag -> listenerName.value,
    NetworkProcessorMetricTag -> id.toString
  ).asJava

  newGauge(IdlePercentMetricName,
    new Gauge[Double] {
      def value = {
        Option(metrics.metric(metrics.metricName("io-wait-ratio", "socket-server-metrics", metricTags)))
          .fold(0.0)(m => Math.min(m.metricValue.asInstanceOf[Double], 1.0))
      }
    },
    // for compatibility, only add a networkProcessor tag to the Yammer Metrics alias (the equivalent Selector metric
    // also includes the listener name)
    Map(NetworkProcessorMetricTag -> id.toString)
  )


  // 选择器
  private val selector = createSelector(
    ChannelBuilders.serverChannelBuilder(listenerName,
      listenerName == config.interBrokerListenerName,
      securityProtocol,
      config,
      credentialProvider.credentialCache,
      credentialProvider.tokenCache))
  // Visible to override for testing 可见覆盖测试

  // Processor的选择器
  protected[network] def createSelector(channelBuilder: ChannelBuilder): KSelector = {
    channelBuilder match {
      case reconfigurable: Reconfigurable => config.addReconfigurable(reconfigurable)
      case _ =>
    }

    new KSelector(
      maxRequestSize,
      connectionsMaxIdleMs,
      metrics,
      time,
      "socket-server",
      metricTags,
      false,
      true,
      channelBuilder,
      memoryPool,
      logContext)
  }

  // Connection ids have the format `localAddr:localPort-remoteAddr:remotePort-index`. The index is a
  // non-negative incrementing value that ensures that even if remotePort is reused after a connection is
  // closed, connection ids are not reused while requests from the closed connection are being processed.
  // 连接ID的格式为“localAddr：localPort-remoteAddr：remotePort-index”。索引是//非负增量值，可确保即使在//连接关闭后重用remotePort，也不会在处理来自已关闭连接的请求时重用连接ID。
  //下一个连接索引
  private var nextConnectionIndex = 0

  // Processor核心处理逻辑
  override def run() {
    startupComplete()
    try {
      while (isRunning) {
        try {
          // setup any new connections that have been queued up
          //这一步把在newConnections中的所有channel注册到selector中去
          configureNewConnections()
          // register any new responses for writing
          //从responseQueue中拿出response进行模式匹配并作响应处理
          processNewResponses()
          //执行selector.poll(300)
          poll()

          processCompletedReceives()
          processCompletedSends()
          processDisconnected()
        } catch {
          // We catch all the throwables here to prevent the processor thread from exiting. We do this because
          // letting a processor exit might cause a bigger impact on the broker. This behavior might need to be
          // reviewed if we see an exception that needs the entire broker to stop. Usually the exceptions thrown would
          // be either associated with a specific socket channel or a bad request. These exceptions are caught and
          // processed by the individual methods above which close the failing channel and continue processing other
          // channels. So this catch block should only ever see ControlThrowables.
          case e: Throwable => processException("Processor got uncaught exception.", e)
        }
      }
    } finally {
      debug("Closing selector - processor " + id)
      CoreUtils.swallow(closeAll(), this, Level.ERROR)
      shutdownComplete()
    }
  }
  // process异常
  private def processException(errorMessage: String, throwable: Throwable) {
    throwable match {
      case e: ControlThrowable => throw e
      case e => error(errorMessage, e)
    }
  }
  // processChannel异常
  private def processChannelException(channelId: String, errorMessage: String, throwable: Throwable) {
    if (openOrClosingChannel(channelId).isDefined) {
      error(s"Closing socket for $channelId because of error", throwable)
      close(channelId)
    }
    processException(errorMessage, throwable)
  }


  /**
    * Register any new connections that have been queued up
    * 注册已排队的所有新连接
    */
  private def configureNewConnections() {
    while (!newConnections.isEmpty) {
      val channel = newConnections.poll()
      try {
        debug(s"Processor $id listening to new connection from ${channel.socket.getRemoteSocketAddress}")
        // 把channel注册到选择器中去
        selector.register(connectionId(channel.socket), channel)
      } catch {
        // We explicitly catch all exceptions and close the socket to avoid a socket leak.
        case e: Throwable =>
          val remoteAddress = channel.socket.getRemoteSocketAddress
          // need to close the channel here to avoid a socket leak.
          close(channel)
          processException(s"Processor $id closed connection from $remoteAddress", e)
      }
    }
  }


  private def processNewResponses() {

    var currentResponse: RequestChannel.Response = null

    while ({currentResponse = dequeueResponse(); currentResponse != null}) {
      // 从响应中拿到channelId
      val channelId = currentResponse.request.context.connectionId

      try {

        //这里就一个模式匹配   匹配是什么类型的Response
        currentResponse match {

          case response: NoOpResponse =>
            // There is no response to send to the client, we need to read more pipelined requests
            // that are sitting in the server's socket buffer
            updateRequestMetrics(response)
            trace("Socket server received empty response to send, registering for read: " + response)
            // Try unmuting the channel. If there was no quota violation and the channel has not been throttled,
            // it will be unmuted immediately. If the channel has been throttled, it will be unmuted only if the
            // throttling delay has already passed by now.
            handleChannelMuteEvent(channelId, ChannelMuteEvent.RESPONSE_SENT)
            tryUnmuteChannel(channelId)

          case response: SendResponse =>
            sendResponse(response, response.responseSend)

          case response: CloseConnectionResponse =>
            updateRequestMetrics(response)
            trace("Closing socket connection actively according to the response code.")
            close(channelId)

          case _: StartThrottlingResponse =>
            handleChannelMuteEvent(channelId, ChannelMuteEvent.THROTTLE_STARTED)

          case _: EndThrottlingResponse =>
            // Try unmuting the channel. The channel will be unmuted only if the response has already been sent out to
            // the client.
            handleChannelMuteEvent(channelId, ChannelMuteEvent.THROTTLE_ENDED)
            tryUnmuteChannel(channelId)

          case _ =>  //抛出异常
            throw new IllegalArgumentException(s"Unknown response type: ${currentResponse.getClass}")
        }


      } catch {
        case e: Throwable =>
          processChannelException(channelId, s"Exception while processing response for $channelId", e)
      }
    }
  }


  private def poll() {
    try selector.poll(300)
    catch {
      case e @ (_: IllegalStateException | _: IOException) =>
        // The exception is not re-thrown and any completed sends/receives/connections/disconnections
        // from this poll will be processed.
        error(s"Processor $id poll failed", e)
    }
  }

  private def processCompletedReceives() {

    selector.completedReceives.asScala.foreach { receive =>
      try {
        openOrClosingChannel(receive.source) match {
          case Some(channel) =>
            val header = RequestHeader.parse(receive.payload)
            val connectionId = receive.source
            val context = new RequestContext(header, connectionId, channel.socketAddress,
              channel.principal, listenerName, securityProtocol)
            val req = new RequestChannel.Request(processor = id, context = context,
              startTimeNanos = time.nanoseconds, memoryPool, receive.payload, requestChannel.metrics)
            requestChannel.sendRequest(req)
            selector.mute(connectionId)
            handleChannelMuteEvent(connectionId, ChannelMuteEvent.REQUEST_RECEIVED)
          case None =>
            // This should never happen since completed receives are processed immediately after `poll()`
            throw new IllegalStateException(s"Channel ${receive.source} removed from selector before processing completed receive")
        }
      } catch {
        // note that even though we got an exception, we can assume that receive.source is valid.
        // Issues with constructing a valid receive object were handled earlier
        case e: Throwable =>
          processChannelException(receive.source, s"Exception while processing request from ${receive.source}", e)
      }
    }
  }



  private def processCompletedSends() {
    selector.completedSends.asScala.foreach { send =>
      try {
        val response = inflightResponses.remove(send.destination).getOrElse {
          throw new IllegalStateException(s"Send for ${send.destination} completed, but not in `inflightResponses`")
        }
        updateRequestMetrics(response)

        // Invoke send completion callback
        response.onComplete.foreach(onComplete => onComplete(send))

        // Try unmuting the channel. If there was no quota violation and the channel has not been throttled,
        // it will be unmuted immediately. If the channel has been throttled, it will unmuted only if the throttling
        // delay has already passed by now.
        handleChannelMuteEvent(send.destination, ChannelMuteEvent.RESPONSE_SENT)
        tryUnmuteChannel(send.destination)
      } catch {
        case e: Throwable => processChannelException(send.destination,
          s"Exception while processing completed send to ${send.destination}", e)
      }
    }
  }


  private def processDisconnected() {
    selector.disconnected.keySet.asScala.foreach { connectionId =>
      try {
        val remoteHost = ConnectionId.fromString(connectionId).getOrElse {
          throw new IllegalStateException(s"connectionId has unexpected format: $connectionId")
        }.remoteHost
        inflightResponses.remove(connectionId).foreach(updateRequestMetrics)
        // the channel has been closed by the selector but the quotas still need to be updated
        connectionQuotas.dec(InetAddress.getByName(remoteHost))
      } catch {
        case e: Throwable => processException(s"Exception while processing disconnection of $connectionId", e)
      }
    }
  }


  /* `protected` for test usage */
  protected[network] def sendResponse(response: RequestChannel.Response, responseSend: Send) {

    val connectionId = response.request.context.connectionId
    trace(s"Socket server received response to send to $connectionId, registering for write and sending data: $response")
    // `channel` can be None if the connection was closed remotely or if selector closed it for being idle for too long

    if (channel(connectionId).isEmpty) {
      warn(s"Attempting to send response via channel for which there is no open connection, connection id 尝试通过没有打开连接的通道发送响应，连接ID$connectionId")
      response.request.updateRequestMetrics(0L, response)
    }
    // Invoke send for closingChannel as well so that the send is failed and the channel closed properly and
    // removed from the Selector after discarding any pending staged receives.
    // `openOrClosingChannel` can be None if the selector closed the connection because it was idle for too long
    if (openOrClosingChannel(connectionId).isDefined) {
      selector.send(responseSend)
      inflightResponses += (connectionId -> response)
    }
  }


  // 更新监控请求
  private def updateRequestMetrics(response: RequestChannel.Response): Unit = {
    val request = response.request
    val networkThreadTimeNanos = openOrClosingChannel(request.context.connectionId).fold(0L)(_.getAndResetNetworkThreadTimeNanos())
    request.updateRequestMetrics(networkThreadTimeNanos, response)
  }



  /**
   * Close the connection identified by `connectionId` and decrement the connection count.
   * The channel will be immediately removed from the selector's `channels` or `closingChannels`
   * and no further disconnect notifications will be sent for this channel by the selector.
   * If responses are pending for the channel, they are dropped and metrics is updated.
   * If the channel has already been removed from selector, no action is taken.
    * 关闭`connectionId`标识的连接并减少连接数。 通道将立即从选择器的“channels”或“closingChannels”中删除，
    * 选择器不会再为此通道发送断开连接通知。 如果通道的响应处于待处理状态，则会删除它们并更新指标。 如果已从选择器中删除该通道，则不执行任何操作。
    *
   */
  private def close(connectionId: String): Unit = {
    openOrClosingChannel(connectionId).foreach { channel =>
      debug(s"Closing selector connection $connectionId")
      val address = channel.socketAddress
      if (address != null)
        connectionQuotas.dec(address)
      selector.close(connectionId)

      inflightResponses.remove(connectionId).foreach(response => updateRequestMetrics(response))
    }
  }

  /**
   * Queue up a new connection for reading
    * 排队一个新的连接进行读， Acceptor调用这个方法把socketChannel放入newConnections中
   */
  def accept(socketChannel: SocketChannel) {
    newConnections.add(socketChannel)
    wakeup()
  }



  // 'protected` to allow override for testing
  protected[network] def connectionId(socket: Socket): String = {
    val localHost = socket.getLocalAddress.getHostAddress
    val localPort = socket.getLocalPort
    val remoteHost = socket.getInetAddress.getHostAddress
    val remotePort = socket.getPort
    val connId = ConnectionId(localHost, localPort, remoteHost, remotePort, nextConnectionIndex).toString
    nextConnectionIndex = if (nextConnectionIndex == Int.MaxValue) 0 else nextConnectionIndex + 1
    connId
  }

  private[network] def enqueueResponse(response: RequestChannel.Response): Unit = {
    responseQueue.put(response)
    wakeup()
  }

  // 从responseQueue中拿出一个Response
  private def dequeueResponse(): RequestChannel.Response = {
    val response = responseQueue.poll()
    if (response != null)
      response.request.responseDequeueTimeNanos = Time.SYSTEM.nanoseconds
    response
  }

  private[network] def responseQueueSize = responseQueue.size

  // Only for testing
  private[network] def inflightResponseCount: Int = inflightResponses.size

  // Visible for testing 可见测试
  // Only methods that are safe to call on a disconnected channel should be invoked on 'openOrClosingChannel'.只应在'openOrClosingChannel'上调用在断开连接的通道上安全调用的方法。
  private[network] def openOrClosingChannel(connectionId: String): Option[KafkaChannel] =
    Option(selector.channel(connectionId)).orElse(Option(selector.closingChannel(connectionId)))

  // Indicate the specified channel that the specified channel mute-related event has happened so that it can change its
  // mute state.
  private def handleChannelMuteEvent(connectionId: String, event: ChannelMuteEvent): Unit = {
    openOrClosingChannel(connectionId).foreach(c => c.handleChannelMuteEvent(event))
  }

  private def tryUnmuteChannel(connectionId: String) = {
    openOrClosingChannel(connectionId).foreach(c => selector.unmute(c.id))
  }

  /* For test usage用于测试用途 */
  private[network] def channel(connectionId: String): Option[KafkaChannel] =
    Option(selector.channel(connectionId))

  // Visible for testing
  private[network] def numStagedReceives(connectionId: String): Int =
    openOrClosingChannel(connectionId).map(c => selector.numStagedReceives(c)).getOrElse(0)


  /**
   * Wakeup the thread for selection.
    * 唤醒selector线程进行选择
   */
  override def wakeup() = selector.wakeup()


  /**
    * Close the selector and all open connections
    * 关闭选择器和所有打开的连接
    */
  private def closeAll() {
    selector.channels.asScala.foreach { channel =>
      close(channel.id)
    }
    selector.close()
    // 移除监控
    removeMetric(IdlePercentMetricName, Map(NetworkProcessorMetricTag -> id.toString))
  }


  // 关闭processor线程并移除监控
  override def shutdown(): Unit = {
    super.shutdown()
    removeMetric("IdlePercent", Map("networkProcessor" -> id.toString))
  }

}





// 链接的配额限制类

class ConnectionQuotas(val defaultMax: Int, overrideQuotas: Map[String, Int]) {
  // 限制每个IP的连接数
  private val overrides = overrideQuotas.map { case (host, count) => (InetAddress.getByName(host), count) }
  // 可变map，存网络地址和连接数的映射
  private val counts = mutable.Map[InetAddress, Int]()
  // 增加
  def inc(address: InetAddress) {
    counts.synchronized {
      val count = counts.getOrElseUpdate(address, 0)
      counts.put(address, count + 1)
      val max = overrides.getOrElse(address, defaultMax)
      // 如果链接太多抛出TooManyConnectionsException异常
      if (count >= max)
        throw new TooManyConnectionsException(address, max)
    }
  }
  // 减少
  def dec(address: InetAddress) {
    counts.synchronized {
      val count = counts.getOrElse(address,
        throw new IllegalArgumentException(s"Attempted to decrease connection count for address with no connections, address: $address"))
      if (count == 1)
        counts.remove(address)
      else
        counts.put(address, count - 1)
    }
  }

  def get(address: InetAddress): Int = counts.synchronized {
    counts.getOrElse(address, 0)
  }

}

class TooManyConnectionsException(val ip: InetAddress, val count: Int) extends KafkaException("来自太多的连接 Too many connections from %s (maximum = %d)".format(ip, count))
