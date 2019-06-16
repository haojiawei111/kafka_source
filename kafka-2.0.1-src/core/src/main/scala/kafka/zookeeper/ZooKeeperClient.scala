/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.zookeeper

import java.util.Locale
import java.util.concurrent.locks.{ReentrantLock, ReentrantReadWriteLock}
import java.util.concurrent.{ArrayBlockingQueue, ConcurrentHashMap, CountDownLatch, Semaphore, TimeUnit}

import com.yammer.metrics.core.{Gauge, MetricName}
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.CoreUtils.{inLock, inReadLock, inWriteLock}
import kafka.utils.{KafkaScheduler, Logging}
import org.apache.kafka.common.utils.Time
import org.apache.zookeeper.AsyncCallback.{ACLCallback, Children2Callback, DataCallback, StatCallback, StringCallback, VoidCallback}
import org.apache.zookeeper.KeeperException.Code
import org.apache.zookeeper.Watcher.Event.{EventType, KeeperState}
import org.apache.zookeeper.ZooKeeper.States
import org.apache.zookeeper.data.{ACL, Stat}
import org.apache.zookeeper.{CreateMode, KeeperException, WatchedEvent, Watcher, ZooKeeper}

import scala.collection.JavaConverters._
import scala.collection.mutable.Set

/**
  * 这个是kafka里面最底层的zk类，KafkaZkClient是它的包装类，提供了一些kafka集群通用的功能
  *
 * A ZooKeeper client that encourages pipelined requests.
 *
 * @param connectString comma separated host:port pairs, each corresponding to a zk server  逗号分隔的主机：端口对，每个对应一个zk服务器
 * @param sessionTimeoutMs session timeout in milliseconds 会话超时（以毫秒为单位）
 * @param connectionTimeoutMs connection timeout in milliseconds
 * @param maxInFlightRequests maximum number of unacknowledged requests the client will send before blocking. 客户端在阻止之前发送的最大未确认请求数。
 */
class ZooKeeperClient(connectString: String,
                      sessionTimeoutMs: Int,
                      connectionTimeoutMs: Int,
                      maxInFlightRequests: Int,
                      time: Time,
                      metricGroup: String,
                      metricType: String) extends Logging with KafkaMetricsGroup {
  this.logIdent = "[ZooKeeperClient] "

  private val initializationLock = new ReentrantReadWriteLock()
  private val isConnectedOrExpiredLock = new ReentrantLock()
  private val isConnectedOrExpiredCondition = isConnectedOrExpiredLock.newCondition()

  // zookeeper节点变化处理程序合集
  private val zNodeChangeHandlers = new ConcurrentHashMap[String, ZNodeChangeHandler]().asScala
  // zookeeper子节点改变处理程序合集
  private val zNodeChildChangeHandlers = new ConcurrentHashMap[String, ZNodeChildChangeHandler]().asScala

  private val inFlightRequests = new Semaphore(maxInFlightRequests)
  private val stateChangeHandlers = new ConcurrentHashMap[String, StateChangeHandler]().asScala
  private[zookeeper] val expiryScheduler = new KafkaScheduler(threads = 1, "zk-session-expiry-handler")

  private val metricNames = Set[String]()

  // The state map has to be created before creating ZooKeeper since it's needed in the ZooKeeper callback.
  //必须在创建ZooKeeper之前创建状态映射，因为ZooKeeper回调需要它。
  private val stateToMeterMap = {
    import KeeperState._
    val stateToEventTypeMap = Map(
      Disconnected -> "Disconnects",
      SyncConnected -> "SyncConnects",
      AuthFailed -> "AuthFailures",
      ConnectedReadOnly -> "ReadOnlyConnects",
      SaslAuthenticated -> "SaslAuthentications",
      Expired -> "Expires"
    )

    //遍历stateToEventTypeMap
    stateToEventTypeMap.map { case (state, eventType) =>
      val name = s"ZooKeeper${eventType}PerSec"
      metricNames += name
      state -> newMeter(name, eventType.toLowerCase(Locale.ROOT), TimeUnit.SECONDS)
    }
  }

  info(s"Initializing a new session to $connectString.")
  // Fail-fast if there's an error during construction (so don't call initialize, which retries forever)
  // 如果在构造期间出现错误，则快速失败（因此不要调用initialize，永远重试）  //这里用zookeeper的包连接zookeeper集群
  @volatile private var zooKeeper = new ZooKeeper(connectString, sessionTimeoutMs, ZooKeeperClientWatcher)// 监控所有被触发的事件

  newGauge("SessionState", new Gauge[String] {
    override def value: String = Option(connectionState.toString).getOrElse("DISCONNECTED")
  })

  metricNames += "SessionState"

  expiryScheduler.startup()

  try waitUntilConnected(connectionTimeoutMs, TimeUnit.MILLISECONDS)
  catch {
    case e: Throwable =>
      close()
      throw e
  }


  override def metricName(name: String, metricTags: scala.collection.Map[String, String]): MetricName = {
    explicitMetricName(metricGroup, metricType, name, metricTags)
  }

  /**
    * 返回zookeeper连接状态
   */
  def connectionState: States = zooKeeper.getState

  /**
   * Send a request and wait for its response. See handle(Seq[AsyncRequest]) for details.
   *
   * @param request a single request to send and wait on.
   * @return an instance of the response with the specific type (e.g. CreateRequest -> CreateResponse).具有特定类型的响应的实例
   */
  def handleRequest[Req <: AsyncRequest](request: Req): Req#Response = {
    handleRequests(Seq(request)).head
  }

  /**
   * 发送一系列流水线请求并等待所有响应。
   *
   * The watch flag on each outgoing request will be set if we've already registered a handler for the
   * path associated with the request.  如果我们已经为与请求关联的路径注册了处理程序，则将设置每个传出请求上的监视标志。
   *
   * @param requests a sequence of requests to send and wait on.
   * @return the responses for the requests. If all requests have the same type, the responses will have the respective
   * response type (e.g. Seq[CreateRequest] -> Seq[CreateResponse]). Otherwise, the most specific common supertype
   * will be used (e.g. Seq[AsyncRequest] -> Seq[AsyncResponse]).
   */
  def handleRequests[Req <: AsyncRequest](requests: Seq[Req]): Seq[Req#Response] = {
    if (requests.isEmpty)
      //返回空集合
      Seq.empty
    else {

      //同步工具类,它允许一个或多个线程一直等待,直到其他线程的操作执行完后再执行
      val countDownLatch = new CountDownLatch(requests.size)
      val responseQueue = new ArrayBlockingQueue[Req#Response](requests.size)

      requests.foreach { request =>
        inFlightRequests.acquire()
        try {
          inReadLock(initializationLock) {
            send(request) { response =>
              //把response添加到responseQueue队列中
              responseQueue.add(response)
              inFlightRequests.release()
              //CountDownLatch减一
              countDownLatch.countDown()
            }
          }
        } catch {
          case e: Throwable =>
            inFlightRequests.release()
            throw e
        }
      }
      countDownLatch.await()
      responseQueue.asScala.toBuffer
    }
  }

  // 这里的最底层的发送逻辑，根据请求类型分别执行不同的zookeeperClient API，注意每个发送请求都会注册回调函数，回调接口是AsyncCallback
  // Visibility to override for testing 覆盖测试的可见性
  private[zookeeper] def send[Req <: AsyncRequest](request: Req)(processResponse: Req#Response => Unit): Unit = {
    // Safe to cast as we always create a response of the right type
    def callback(response: AsyncResponse): Unit = processResponse(response.asInstanceOf[Req#Response])

    def responseMetadata(sendTimeMs: Long) = new ResponseMetadata(sendTimeMs, receivedTimeMs = time.hiResClockMs())

    //zookeeper发送request
    val sendTimeMs = time.hiResClockMs()
    request match {

      case ExistsRequest(path, ctx) =>
        zooKeeper.exists(path, shouldWatch(request), new StatCallback {
          override def processResult(rc: Int, path: String, ctx: Any, stat: Stat): Unit =
            callback(ExistsResponse(Code.get(rc), path, Option(ctx), stat, responseMetadata(sendTimeMs)))
        }, ctx.orNull)

      case GetDataRequest(path, ctx) =>
        zooKeeper.getData(path, shouldWatch(request), new DataCallback {
          override def processResult(rc: Int, path: String, ctx: Any, data: Array[Byte], stat: Stat): Unit =
            callback(GetDataResponse(Code.get(rc), path, Option(ctx), data, stat, responseMetadata(sendTimeMs)))
        }, ctx.orNull)

      case GetChildrenRequest(path, ctx) =>
        zooKeeper.getChildren(path, shouldWatch(request), new Children2Callback {
          override def processResult(rc: Int, path: String, ctx: Any, children: java.util.List[String], stat: Stat): Unit =
            callback(GetChildrenResponse(Code.get(rc), path, Option(ctx),
              Option(children).map(_.asScala).getOrElse(Seq.empty), stat, responseMetadata(sendTimeMs)))
        }, ctx.orNull)

      case CreateRequest(path, data, acl, createMode, ctx) =>
        zooKeeper.create(path, data, acl.asJava, createMode, new StringCallback {
          override def processResult(rc: Int, path: String, ctx: Any, name: String): Unit =
            callback(CreateResponse(Code.get(rc), path, Option(ctx), name, responseMetadata(sendTimeMs)))
        }, ctx.orNull)

      case SetDataRequest(path, data, version, ctx) =>
        zooKeeper.setData(path, data, version, new StatCallback {
          override def processResult(rc: Int, path: String, ctx: Any, stat: Stat): Unit =
            callback(SetDataResponse(Code.get(rc), path, Option(ctx), stat, responseMetadata(sendTimeMs)))
        }, ctx.orNull)

      case DeleteRequest(path, version, ctx) =>
        zooKeeper.delete(path, version, new VoidCallback {
          override def processResult(rc: Int, path: String, ctx: Any): Unit =
            callback(DeleteResponse(Code.get(rc), path, Option(ctx), responseMetadata(sendTimeMs)))
        }, ctx.orNull)

      case GetAclRequest(path, ctx) =>
        zooKeeper.getACL(path, null, new ACLCallback {
          override def processResult(rc: Int, path: String, ctx: Any, acl: java.util.List[ACL], stat: Stat): Unit = {
            callback(GetAclResponse(Code.get(rc), path, Option(ctx), Option(acl).map(_.asScala).getOrElse(Seq.empty),
              stat, responseMetadata(sendTimeMs)))
        }}, ctx.orNull)

      case SetAclRequest(path, acl, version, ctx) =>
        zooKeeper.setACL(path, acl.asJava, version, new StatCallback {
          override def processResult(rc: Int, path: String, ctx: Any, stat: Stat): Unit =
            callback(SetAclResponse(Code.get(rc), path, Option(ctx), stat, responseMetadata(sendTimeMs)))
        }, ctx.orNull)

    }
  }




  /**
   * Wait indefinitely until the underlying zookeeper client to reaches the CONNECTED state.
    *  无限期等待，直到基础zookeeper客户端达到CONNECTED状态。
   * @throws ZooKeeperClientAuthFailedException if the authentication failed either before or while waiting for connection.
   * @throws ZooKeeperClientExpiredException if the session expired either before or while waiting for connection.
   */
  def waitUntilConnected(): Unit = inLock(isConnectedOrExpiredLock) {
    waitUntilConnected(Long.MaxValue, TimeUnit.MILLISECONDS)
  }

  private def waitUntilConnected(timeout: Long, timeUnit: TimeUnit): Unit = {
    info("Waiting until connected.")
    var nanos = timeUnit.toNanos(timeout)
    inLock(isConnectedOrExpiredLock) {
      var state = connectionState
      while (!state.isConnected && state.isAlive) {
        if (nanos <= 0) {
          throw new ZooKeeperClientTimeoutException(s"Timed out waiting for connection while in state: $state")
        }
        nanos = isConnectedOrExpiredCondition.awaitNanos(nanos)
        state = connectionState
      }
      if (state == States.AUTH_FAILED) {
        throw new ZooKeeperClientAuthFailedException("Auth failed either before or while waiting for connection")
      } else if (state == States.CLOSED) {
        throw new ZooKeeperClientExpiredException("Session expired either before or while waiting for connection")
      }
    }
    info("Connected.")
  }

  // If this method is changed, the documentation for registerZNodeChangeHandler and/or registerZNodeChildChangeHandler
  // may need to be updated.
  // 如果更改此方法，则可能需要更新registerZNodeChangeHandler和/或registerZNodeChildChangeHandler的文档。  注册今天
  private def shouldWatch(request: AsyncRequest): Boolean = request match {
    case _: GetChildrenRequest => zNodeChildChangeHandlers.contains(request.path)
    case _: ExistsRequest | _: GetDataRequest => zNodeChangeHandlers.contains(request.path)
    case _ => throw new IllegalArgumentException(s"Request $request is not watchable")
  }

  /**
   * Register the handler to ZooKeeperClient. This is just a local operation. This does not actually register a watcher.
   *将处理程序注册到ZooKeeperClient。这只是一个本地操作。这实际上并没有注册观察者。
   * The watcher is only registered once the user calls handle(AsyncRequest) or handle(Seq[AsyncRequest])
   * with either a GetDataRequest or ExistsRequest.
   *
   * NOTE: zookeeper only allows registration to a nonexistent znode with ExistsRequest.
   *
   * @param zNodeChangeHandler the handler to register
   */
  def registerZNodeChangeHandler(zNodeChangeHandler: ZNodeChangeHandler): Unit = {
    zNodeChangeHandlers.put(zNodeChangeHandler.path, zNodeChangeHandler)
  }

  /**
   * Unregister the handler from ZooKeeperClient. This is just a local operation.
   * @param path the path of the handler to unregister
   */
  def unregisterZNodeChangeHandler(path: String): Unit = {
    zNodeChangeHandlers.remove(path)
  }

  /**
   * Register the handler to ZooKeeperClient. This is just a local operation. This does not actually register a watcher.
   *
   * The watcher is only registered once the user calls handle(AsyncRequest) or handle(Seq[AsyncRequest]) with a GetChildrenRequest.
   *
   * @param zNodeChildChangeHandler the handler to register
   */
  def registerZNodeChildChangeHandler(zNodeChildChangeHandler: ZNodeChildChangeHandler): Unit = {
    zNodeChildChangeHandlers.put(zNodeChildChangeHandler.path, zNodeChildChangeHandler)
  }

  /**
   * Unregister the handler from ZooKeeperClient. This is just a local operation.
   * @param path the path of the handler to unregister
   */
  def unregisterZNodeChildChangeHandler(path: String): Unit = {
    zNodeChildChangeHandlers.remove(path)
  }

  /**
   * @param stateChangeHandler
   */
  def registerStateChangeHandler(stateChangeHandler: StateChangeHandler): Unit = inReadLock(initializationLock) {
    if (stateChangeHandler != null)
      stateChangeHandlers.put(stateChangeHandler.name, stateChangeHandler)
  }

  /**
   *
   * @param name
   */
  def unregisterStateChangeHandler(name: String): Unit = inReadLock(initializationLock) {
    stateChangeHandlers.remove(name)
  }

  def close(): Unit = {
    info("Closing.")
    inWriteLock(initializationLock) {
      zNodeChangeHandlers.clear()
      zNodeChildChangeHandlers.clear()
      stateChangeHandlers.clear()
      zooKeeper.close()
      metricNames.foreach(removeMetric(_))
    }
    // 如果调度程序正在等待锁定以处理会话到期，则在锁定之外关闭调度程序以避免死锁
    // Shutdown scheduler outside of lock to avoid deadlock if scheduler
    // is waiting for lock to process session expiry
    expiryScheduler.shutdown()
    info("Closed.")
  }

  def sessionId: Long = inReadLock(initializationLock) {
    zooKeeper.getSessionId
  }

  // Only for testing
  private[kafka] def currentZooKeeper: ZooKeeper = inReadLock(initializationLock) {
    zooKeeper
  }

  /**
    * zookeeperClient 调度函数
    */
  private def reinitialize(): Unit = {
    // Initialization callbacks are invoked outside of the lock to avoid deadlock potential since their completion
    // may require additional Zookeeper requests, which will block to acquire the initialization lock
    stateChangeHandlers.values.foreach(callBeforeInitializingSession _)

    inWriteLock(initializationLock) {
      if (!connectionState.isAlive) {
        zooKeeper.close()
        info(s"Initializing a new session to $connectString.")
        // retry forever until ZooKeeper can be instantiated
        var connected = false
        while (!connected) {
          try {
            zooKeeper = new ZooKeeper(connectString, sessionTimeoutMs, ZooKeeperClientWatcher)
            connected = true
          } catch {
            case e: Exception =>
              info("Error when recreating ZooKeeper, retrying after a short sleep", e)
              Thread.sleep(1000)
          }
        }
      }
    }

    stateChangeHandlers.values.foreach(callAfterInitializingSession _)
  }

  /**
   * Close the zookeeper client to force session reinitialization. This is visible for testing only.
    * 关闭zookeeper客户端以强制重新初始化会话。这仅用于测试。
   */
  private[zookeeper] def forceReinitialize(): Unit = {
    zooKeeper.close()
    reinitialize()
  }

  //在初始化会话之前调用 zookeeper连接重置的时候调用
  private def callBeforeInitializingSession(handler: StateChangeHandler): Unit = {
    try {
      handler.beforeInitializingSession()
    } catch {
      case t: Throwable =>
        error(s"Uncaught error in handler ${handler.name}", t)
    }
  }
  //在初始化会话之后调用 zookeeper连接重置的时候调用
  private def callAfterInitializingSession(handler: StateChangeHandler): Unit = {
    try {
      handler.afterInitializingSession()
    } catch {
      case t: Throwable =>
        error(s"Uncaught error in handler ${handler.name}", t)
    }
  }

  // Visibility for testing
  //zookeeper会话到期处理程序
  private[zookeeper] def scheduleSessionExpiryHandler(): Unit = {
    //调度一次并且立即执行
    expiryScheduler.scheduleOnce("zk-session-expired", () => {
      info("Session expired.")
      reinitialize() //从新初始化
    })
  }

  // 这里再注册zookeeper监听的
  // package level visibility for testing only
  private[zookeeper] object ZooKeeperClientWatcher extends Watcher {
    override def process(event: WatchedEvent): Unit = {
      debug(s"Received event: $event")
      Option(event.getPath) match {
        case None =>
          val state = event.getState
          stateToMeterMap.get(state).foreach(_.mark())
          inLock(isConnectedOrExpiredLock) {
            isConnectedOrExpiredCondition.signalAll()
          }
          if (state == KeeperState.AuthFailed) {
            error("Auth failed.")
            stateChangeHandlers.values.foreach(_.onAuthFailure())
          } else if (state == KeeperState.Expired) {//zookeeper会话到期处理程序，重新连接
            scheduleSessionExpiryHandler()
          }
        case Some(path) =>
          (event.getType: @unchecked) match {
              // 子节点变化
            case EventType.NodeChildrenChanged => zNodeChildChangeHandlers.get(path).foreach(_.handleChildChange())
              // 节点创建
            case EventType.NodeCreated => zNodeChangeHandlers.get(path).foreach(_.handleCreation())
              // 节点删除
            case EventType.NodeDeleted => zNodeChangeHandlers.get(path).foreach(_.handleDeletion())
              // 节点数据改变
            case EventType.NodeDataChanged => zNodeChangeHandlers.get(path).foreach(_.handleDataChange())
          }
      }
    }
  }
}


//zookeeper 处理类接口

trait StateChangeHandler {
  val name: String
  def beforeInitializingSession(): Unit = {}
  def afterInitializingSession(): Unit = {}
  def onAuthFailure(): Unit = {}
}


trait ZNodeChangeHandler {
  val path: String
  def handleCreation(): Unit = {}
  def handleDeletion(): Unit = {}
  def handleDataChange(): Unit = {}
}


trait ZNodeChildChangeHandler {
  val path: String
  def handleChildChange(): Unit = {}
}


// 一下是zookeeper的请求和响应，请求和响应一一对应

// zookeeper请求

sealed trait AsyncRequest {
  /**
   * This type member allows us to define methods that take requests and return responses with the correct types.
   * See ``ZooKeeperClient.handleRequests`` for example.
    * 此类型成员允许我们定义接收请求并返回具有正确类型的响应的方法。 *例如，参见``ZooKeeperClient.handleRequests``。
   */
  type Response <: AsyncResponse
  def path: String
  def ctx: Option[Any]
}

//创建zk path请求
case class CreateRequest(path: String, data: Array[Byte], acl: Seq[ACL], createMode: CreateMode,
                         ctx: Option[Any] = None) extends AsyncRequest {
  type Response = CreateResponse
}
//删除zk path请求
case class DeleteRequest(path: String, version: Int, ctx: Option[Any] = None) extends AsyncRequest {
  type Response = DeleteResponse
}
//是否存在zk path请求
case class ExistsRequest(path: String, ctx: Option[Any] = None) extends AsyncRequest {
  type Response = ExistsResponse
}
//得到zk path的数据的请求
case class GetDataRequest(path: String, ctx: Option[Any] = None) extends AsyncRequest {
  type Response = GetDataResponse
}

//设置数据到zk path的请求
case class SetDataRequest(path: String, data: Array[Byte], version: Int, ctx: Option[Any] = None) extends AsyncRequest {
  type Response = SetDataResponse
}
// 拿到Acl请求
case class GetAclRequest(path: String, ctx: Option[Any] = None) extends AsyncRequest {
  type Response = GetAclResponse
}
// 设置Acl 请求
case class SetAclRequest(path: String, acl: Seq[ACL], version: Int, ctx: Option[Any] = None) extends AsyncRequest {
  type Response = SetAclResponse
}
// 得到zk path子节点
case class GetChildrenRequest(path: String, ctx: Option[Any] = None) extends AsyncRequest {
  type Response = GetChildrenResponse
}


// zookeeper Response   异步响应

sealed abstract class AsyncResponse {
  def resultCode: Code
  def path: String
  def ctx: Option[Any]

  /** 如果结果代码是OK则返回None，否则返回KeeperException。*/
  def resultException: Option[KeeperException] =
    if (resultCode == Code.OK) None else Some(KeeperException.create(resultCode, path))

  /**
   * 如果结果代码不正确，则抛出KeeperException。
   */
  def maybeThrow(): Unit = {
    if (resultCode != Code.OK)
      throw KeeperException.create(resultCode, path)
  }

  def metadata: ResponseMetadata
}

case class ResponseMetadata(sendTimeMs: Long, receivedTimeMs: Long) {
  def responseTimeMs: Long = receivedTimeMs - sendTimeMs
}

case class CreateResponse(resultCode: Code, path: String, ctx: Option[Any], name: String, metadata: ResponseMetadata) extends AsyncResponse

case class DeleteResponse(resultCode: Code, path: String, ctx: Option[Any], metadata: ResponseMetadata) extends AsyncResponse

case class ExistsResponse(resultCode: Code, path: String, ctx: Option[Any], stat: Stat, metadata: ResponseMetadata) extends AsyncResponse

case class GetDataResponse(resultCode: Code, path: String, ctx: Option[Any], data: Array[Byte], stat: Stat,
                           metadata: ResponseMetadata) extends AsyncResponse

case class SetDataResponse(resultCode: Code, path: String, ctx: Option[Any], stat: Stat, metadata: ResponseMetadata) extends AsyncResponse

case class GetAclResponse(resultCode: Code, path: String, ctx: Option[Any], acl: Seq[ACL], stat: Stat,
                          metadata: ResponseMetadata) extends AsyncResponse

case class SetAclResponse(resultCode: Code, path: String, ctx: Option[Any], stat: Stat, metadata: ResponseMetadata) extends AsyncResponse

case class GetChildrenResponse(resultCode: Code, path: String, ctx: Option[Any], children: Seq[String], stat: Stat,
                               metadata: ResponseMetadata) extends AsyncResponse

// zk 异常类
class ZooKeeperClientException(message: String) extends RuntimeException(message)
class ZooKeeperClientExpiredException(message: String) extends ZooKeeperClientException(message)
class ZooKeeperClientAuthFailedException(message: String) extends ZooKeeperClientException(message)
class ZooKeeperClientTimeoutException(message: String) extends ZooKeeperClientException(message)
