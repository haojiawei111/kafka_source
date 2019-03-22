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
package kafka.utils

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import kafka.utils.json.JsonValue

import scala.collection._
import scala.reflect.ClassTag

/**
 * Provides methods for parsing JSON with Jackson and encoding to JSON with a simple and naive custom implementation.
  * 提供了使用Jackson解析JSON并使用简单而天真的自定义实现编码为JSON的方法。
 */
object Json {

  private val mapper = new ObjectMapper()

  /**
   * Parse a JSON string into a JsonValue if possible. `None` is returned if `input` is not valid JSON.
   */
  def parseFull(input: String): Option[JsonValue] =
    try Option(mapper.readTree(input)).map(JsonValue(_))
    catch {
      case _: JsonProcessingException =>
        // Before 1.0.1, Json#encode did not escape backslash or any other special characters. SSL principals
        // stored in ACLs may contain backslash as an escape char, making the JSON generated in earlier versions invalid.
        // Escape backslash and retry to handle these strings which may have been persisted in ZK.
        // Note that this does not handle all special characters (e.g. non-escaped double quotes are not supported)
        //在1.0.1之前，Json #coding没有转义反斜杠或任何其他特殊字符。存储在ACL中的SSL主体可能包含反斜杠作为转义字符，使早期版本中生成的JSON无效。
        // 转义反斜杠并重试处理这些可能在ZK中保留的字符串。
        // 请注意，这不会处理所有特殊字符（例如，不支持非转义双引号）
        val escapedInput = input.replaceAll("\\\\", "\\\\\\\\")
        try Option(mapper.readTree(escapedInput)).map(JsonValue(_))
        catch { case _: JsonProcessingException => None }
    }

  /**
   * Parse a JSON string into either a generic type T, or a JsonProcessingException in the case of
   * exception.
   */
  def parseStringAs[T](input: String)(implicit tag: ClassTag[T]): Either[JsonProcessingException, T] = {
    try Right(mapper.readValue(input, tag.runtimeClass).asInstanceOf[T])
    catch { case e: JsonProcessingException => Left(e) }
  }

  /**
   * Parse a JSON byte array into a JsonValue if possible. `None` is returned if `input` is not valid JSON.
   */
  def parseBytes(input: Array[Byte]): Option[JsonValue] =
    try Option(mapper.readTree(input)).map(JsonValue(_))
    catch { case _: JsonProcessingException => None }

  def tryParseBytes(input: Array[Byte]): Either[JsonProcessingException, JsonValue] =
    try Right(mapper.readTree(input)).right.map(JsonValue(_))
    catch { case e: JsonProcessingException => Left(e) }

  /**
   * Parse a JSON byte array into either a generic type T, or a JsonProcessingException in the case of exception.
   */
  def parseBytesAs[T](input: Array[Byte])(implicit tag: ClassTag[T]): Either[JsonProcessingException, T] = {
    try Right(mapper.readValue(input, tag.runtimeClass).asInstanceOf[T])
    catch { case e: JsonProcessingException => Left(e) }
  }

  /**
   * Encode an object into a JSON string. This method accepts any type T where
   *   T => null | Boolean | String | Number | Map[String, T] | Array[T] | Iterable[T]
   * Any other type will result in an exception.
   * 
   * This implementation is inefficient, so we recommend `encodeAsString` or `encodeAsBytes` (the latter is preferred
   * if possible). This method supports scala Map implementations while the other two do not. Once this functionality
   * is no longer required, we can remove this method.
   */
  def legacyEncodeAsString(obj: Any): String = {
    obj match {
      case null => "null"
      case b: Boolean => b.toString
      case s: String => mapper.writeValueAsString(s)
      case n: Number => n.toString
      case m: Map[_, _] => "{" +
        m.map {
          case (k, v) => legacyEncodeAsString(k) + ":" + legacyEncodeAsString(v)
          case elem => throw new IllegalArgumentException(s"Invalid map element '$elem' in $obj")
        }.mkString(",") + "}"
      case a: Array[_] => legacyEncodeAsString(a.toSeq)
      case i: Iterable[_] => "[" + i.map(legacyEncodeAsString).mkString(",") + "]"
      case other: AnyRef => throw new IllegalArgumentException(s"Unknown argument of type ${other.getClass}: $other")
    }
  }

  /**
    * 将对象编码为JSON字符串。此方法接受默认配置中Jackson的ObjectMapper支持的任何类型。也就是说，支持Java集合，但Scala集合不支持（以避免* jackson-scala依赖）。
   */
  def encodeAsString(obj: Any): String = mapper.writeValueAsString(obj)

  /**
    * 将对象编码为以字节为单位的JSON值。此方法接受默认配置中Jackson的ObjectMapper支持的任何类型。也就是说，支持Java集合，但Scala集合不支持（以避免* jackson-scala依赖）。
   */
  def encodeAsBytes(obj: Any): Array[Byte] = mapper.writeValueAsBytes(obj)
}
