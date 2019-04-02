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

package kafka.server

import java.nio.charset.StandardCharsets

import kafka.common.{NotificationHandler, ZkNodeChangeNotificationListener}
import kafka.utils.{Json, Logging}
import kafka.utils.json.JsonObject
import kafka.zk.{AdminZkClient, ConfigEntityChangeNotificationSequenceZNode, ConfigEntityChangeNotificationZNode, KafkaZkClient}
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.utils.Time

import scala.collection.JavaConverters._
import scala.collection._

/**
 * Represents all the entities that can be configured via ZK
  * 表示可以通过ZK配置的所有实体
 */
object ConfigType {
  val Topic = "topics"
  val Client = "clients"
  val User = "users"
  val Broker = "brokers"
  val all = Seq(Topic, Client, User, Broker)
}

object ConfigEntityName {
  val Default = "<default>"
}

/**
 * This class initiates and carries out config changes for all entities defined in ConfigType.
 *此类为ConfigType中定义的所有实体启动并执行配置更改。
 * It works as follows.它的工作原理如下。
 *
 * 配置存储在路径下: /config/entityType/entityName
 *   E.g. /config/topics/<topic_name> and /config/clients/<clientId>
 * This znode stores the overrides for this entity in properties format with defaults stored using entityName "<default>".
  * 这个节点对于属性格式的此实体，使用entityName“<default>”存储默认值。
  *
 * Multiple entity names may be specified (eg. <user, client-id> quotas) using a hierarchical path:
  * 可以使用分层路径指定多个实体名称（例如，<user，client-id> quotas）
 *   E.g. /config/users/<user>/clients/<clientId>
 *
 * To avoid watching all topics for changes instead we have a notification path
  * 为了避免看到所有主题的变化，我们有一个通知路径
 *   /config/changes
 * The DynamicConfigManager has a child watch on this path.
  * DynamicConfigManager在此路径上有一个子监视。
 *
 * To update a config we first update the config properties. Then we create a new sequential
  * 要更新配置，我们首先更新配置属性。然后我们在更改路径下创建一个新的顺序
 * znode under the change path which contains the name of the entityType and entityName that was updated, say
 * 更改路径下的znode，其中包含更新的entityType和entityName的名称
 *   /config/changes/config_change_13321
 * The sequential znode contains data in this format顺序znode包含此格式的数据: {"version" : 1, "entity_type":"topic/client", "entity_name" : "topic_name/client_id"}
 * This is just a notification--the actual config change is stored only once under the /config/entityType/entityName path.
  * 这只是一个通知 - 实际的配置更改仅在/ config / entityType / entityName路径下存储一次。
 * Version 2 of notifications has the format通知版本2具有格式: {"version" : 2, "entity_path":"entity_type/entity_name"}
 * Multiple entities may be specified as a hierarchical path (eg. users/<user>/clients/<clientId>).
  * 可以将多个实体指定为分层路径（例如，users / <user> / clients / <clientId>）。
 *
 * This will fire a watcher on all brokers. This watcher works as follows. It reads all the config change notifications.
 * It keeps track of the highest config change suffix number it has applied previously. For any previously applied change it finds
 * it checks if this notification is larger than a static expiration time (say 10mins) and if so it deletes this notification.
 * For any new changes it reads the new configuration, combines it with the defaults, and updates the existing config.
 *
 * Note that config is always read from the config path in zk, the notification is just a trigger to do so. So if a broker is
 * down and misses a change that is fine--when it restarts it will be loading the full config anyway. Note also that
 * if there are two consecutive config changes it is possible that only the last one will be applied (since by the time the
 * broker reads the config the both changes may have been made). In this case the broker would needlessly refresh the config twice,
 * but that is harmless.
 *
 * On restart the config manager re-processes all notifications. This will usually be wasted work, but avoids any race conditions
 * on startup where a change might be missed between the initial config load and registering for change notifications.
  * 重新启动时，配置管理器会重新处理所有通知。这通常是浪费的工作，但避免了启动时的任何竞争条件*，在初始配置加载和注册更改通知之间可能会错过更改。
 *
 */
class DynamicConfigManager(private val zkClient: KafkaZkClient,
                           private val configHandlers: Map[String, ConfigHandler],
                           private val changeExpirationMs: Long = 15*60*1000,
                           private val time: Time = Time.SYSTEM) extends Logging {

  val adminZkClient = new AdminZkClient(zkClient)

  object ConfigChangedNotificationHandler extends NotificationHandler {

    override def processNotification(jsonBytes: Array[Byte]) = {
      // Ignore non-json notifications because they can be from the deprecated TopicConfigManager
      Json.parseBytes(jsonBytes).foreach { js =>
        val jsObject = js.asJsonObjectOption.getOrElse {
          throw new IllegalArgumentException("Config change notification has an unexpected value. The format is:" +
            """{"version" : 1, "entity_type":"topics/clients", "entity_name" : "topic_name/client_id"} or """ +
            """{"version" : 2, "entity_path":"entity_type/entity_name"}. """ +
            s"Received: ${new String(jsonBytes, StandardCharsets.UTF_8)}")
        }
        jsObject("version").to[Int] match {
          case 1 => processEntityConfigChangeVersion1(jsonBytes, jsObject)
          case 2 => processEntityConfigChangeVersion2(jsonBytes, jsObject)
          case version => throw new IllegalArgumentException("Config change notification has unsupported version " +
            s"'$version', supported versions are 1 and 2.")
        }
      }
    }


    private def processEntityConfigChangeVersion1(jsonBytes: Array[Byte], js: JsonObject) {
      val validConfigTypes = Set(ConfigType.Topic, ConfigType.Client)
      val entityType = js.get("entity_type").flatMap(_.to[Option[String]]).filter(validConfigTypes).getOrElse {
        throw new IllegalArgumentException("Version 1 config change notification must have 'entity_type' set to " +
          s"'clients' or 'topics'. Received: ${new String(jsonBytes, StandardCharsets.UTF_8)}")
      }

      val entity = js.get("entity_name").flatMap(_.to[Option[String]]).getOrElse {
        throw new IllegalArgumentException("Version 1 config change notification does not specify 'entity_name'. " +
          s"Received: ${new String(jsonBytes, StandardCharsets.UTF_8)}")
      }

      val entityConfig = adminZkClient.fetchEntityConfig(entityType, entity)
      info(s"Processing override for entityType: $entityType, entity: $entity with config: $entityConfig")
      configHandlers(entityType).processConfigChanges(entity, entityConfig)

    }


    private def processEntityConfigChangeVersion2(jsonBytes: Array[Byte], js: JsonObject) {

      val entityPath = js.get("entity_path").flatMap(_.to[Option[String]]).getOrElse {
        throw new IllegalArgumentException(s"Version 2 config change notification must specify 'entity_path'. " +
          s"Received: ${new String(jsonBytes, StandardCharsets.UTF_8)}")
      }

      val index = entityPath.indexOf('/')
      val rootEntityType = entityPath.substring(0, index)
      if (index < 0 || !configHandlers.contains(rootEntityType)) {
        val entityTypes = configHandlers.keys.map(entityType => s"'$entityType'/").mkString(", ")
        throw new IllegalArgumentException("Version 2 config change notification must have 'entity_path' starting with " +
          s"one of $entityTypes. Received: ${new String(jsonBytes, StandardCharsets.UTF_8)}")
      }
      val fullSanitizedEntityName = entityPath.substring(index + 1)

      val entityConfig = adminZkClient.fetchEntityConfig(rootEntityType, fullSanitizedEntityName)
      val loggableConfig = entityConfig.asScala.map {
        case (k, v) => (k, if (ScramMechanism.isScram(k)) Password.HIDDEN else v)
      }
      info(s"Processing override for entityPath: $entityPath with config: $loggableConfig")
      configHandlers(rootEntityType).processConfigChanges(fullSanitizedEntityName, entityConfig)

    }

  }

  // 动态配置核心逻辑
  private val configChangeListener = new ZkNodeChangeNotificationListener(
    zkClient,
    ConfigEntityChangeNotificationZNode.path,// 监听zookeeper上/config/changes路径
    ConfigEntityChangeNotificationSequenceZNode.SequenceNumberPrefix,  // 序列号前缀config_change_
    ConfigChangedNotificationHandler)

  /**
   * 开始观察配置更改
   */
  def startup(): Unit = {

    configChangeListener.init()

    // Apply all existing client/user configs to the ClientIdConfigHandler/UserConfigHandler to bootstrap the overrides
    // 将所有现有客户端/用户配置应用于ClientIdConfigHandler / UserConfigHandler以引导覆盖
    configHandlers.foreach {

      case (ConfigType.User, handler) =>
        adminZkClient.fetchAllEntityConfigs(ConfigType.User).foreach {
          case (sanitizedUser, properties) => handler.processConfigChanges(sanitizedUser, properties)
        }
        adminZkClient.fetchAllChildEntityConfigs(ConfigType.User, ConfigType.Client).foreach {
          case (sanitizedUserClientId, properties) => handler.processConfigChanges(sanitizedUserClientId, properties)
        }

      case (configType, handler) =>
        adminZkClient.fetchAllEntityConfigs(configType).foreach {
          case (entityName, properties) => handler.processConfigChanges(entityName, properties)
        }

    }
  }


  def shutdown(): Unit = {
    configChangeListener.close()
  }
}
