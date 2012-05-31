package com.linkedin.norbert
package network
package util

import com.google.protobuf.ByteString
import logging.Logging
import java.lang.reflect.{Field, Constructor}

/**
 * A wrapper for converting from byte[] <-> ByteString. Protocol buffers makes unnecessary
 * defensive copies at each conversion, and this class encapsulates logic using reflection
 * to bypass those.
 */
object ProtoUtils extends Logging {
  private val byteStringConstructor: Constructor[ByteString] = try {
    val c = classOf[ByteString].getDeclaredConstructor(classOf[Array[Byte]])
    c.setAccessible(true)
    c
  } catch {
    case ex: Exception =>
      log.warn(ex, "Cannot eliminate a copy when converting a byte[] to a ByteString")
      null
  }

  private val byteStringField: Field = try {
    val f = classOf[ByteString].getDeclaredField("bytes")
    f.setAccessible(true)
    f
  } catch {
    case ex: Exception =>
      log.warn(ex, "Cannot eliminate a copy when converting a ByteString to a byte[]")
      null
  }

  def byteArrayToByteString(byteArray: Array[Byte], avoidByteStringCopy: Boolean): ByteString = {
    if(avoidByteStringCopy)
      fastByteArrayToByteString(byteArray)
    else
      slowByteArrayToByteString(byteArray)
  }

  def byteStringToByteArray(byteString: ByteString, avoidByteStringCopy: Boolean): Array[Byte] = {
    if(avoidByteStringCopy)
      fastByteStringToByteArray(byteString)
    else
      slowByteStringToByteArray(byteString)
  }

  private final def fastByteArrayToByteString(byteArray: Array[Byte]): ByteString = {
    if(byteStringConstructor != null)
      try {
        byteStringConstructor.newInstance(byteArray)
      } catch {
        case ex: Exception =>
          log.warn(ex, "Encountered exception invoking the private ByteString constructor, falling back to safe method")
          slowByteArrayToByteString(byteArray)
      }
    else
      slowByteArrayToByteString(byteArray)
  }

  private final def slowByteArrayToByteString(byteArray: Array[Byte]): ByteString = {
    ByteString.copyFrom(byteArray)
  }

  private final def fastByteStringToByteArray(byteString: ByteString): Array[Byte] = {
    if(byteStringField != null)
      try {
        byteStringField.get(byteString).asInstanceOf[Array[Byte]]
      } catch {
        case ex: Exception =>
          log.warn(ex, "Encountered exception accessing the private ByteString bytes field, falling back to safe method")
          slowByteStringToByteArray(byteString)
      }
    else
      slowByteStringToByteArray(byteString)
  }

  private final def slowByteStringToByteArray(byteString: ByteString): Array[Byte] = {
    byteString.toByteArray
  }
}