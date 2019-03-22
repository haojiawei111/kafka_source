package kafka.cluster

import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.Utils

import scala.collection.Map

object EndPoint {

  private val uriParseExp = """^(.*)://\[?([0-9a-zA-Z\-%._:]*)\]?:(-?[0-9]+)""".r

  private[kafka] val DefaultSecurityProtocolMap: Map[ListenerName, SecurityProtocol] = SecurityProtocol.values.map(sp => ListenerName.forSecurityProtocol(sp) -> sp).toMap

  /**
   * Create EndPoint object from `connectionString` and optional `securityProtocolMap`. If the latter is not provided,
   * we fallback to the default behaviour where listener names are the same as security protocols.
   *
   * @param connectionString the format is listener_name://host:port or listener_name://[ipv6 host]:port
   *                         for example: PLAINTEXT://myhost:9092, CLIENT://myhost:9092 or REPLICATION://[::1]:9092
   *                         Host can be empty (PLAINTEXT://:9092) in which case we'll bind to default interface
   *                         Negative ports are also accepted, since they are used in some unit tests
   */
  def createEndPoint(connectionString: String, securityProtocolMap: Option[Map[ListenerName, SecurityProtocol]]): EndPoint = {
    val protocolMap = securityProtocolMap.getOrElse(DefaultSecurityProtocolMap)

    //安全协议
    def securityProtocol(listenerName: ListenerName): SecurityProtocol =
      protocolMap.getOrElse(listenerName,
        throw new IllegalArgumentException(s"No security protocol defined for listener ${listenerName.value}"))

    connectionString match {
      case uriParseExp(listenerNameString, "", port) =>
        val listenerName = ListenerName.normalised(listenerNameString)
        new EndPoint(null, port.toInt, listenerName, securityProtocol(listenerName))
      case uriParseExp(listenerNameString, host, port) =>
        val listenerName = ListenerName.normalised(listenerNameString)
        new EndPoint(host, port.toInt, listenerName, securityProtocol(listenerName))
      case _ => throw new KafkaException(s"Unable to parse $connectionString to a broker endpoint")
    }
  }
}

/**
 * Part of the broker definition - matching host/port pair to a protocol
  * broker定义的一部分 - 将主机/端口对与协议匹配   listenerName协议名称
 */
case class EndPoint(host: String, port: Int, listenerName: ListenerName, securityProtocol: SecurityProtocol) {
  def connectionString: String = {
    val hostport =
      if (host == null)
        ":"+port
      else
        Utils.formatAddress(host, port)
    listenerName.value + "://" + hostport
  }
}
