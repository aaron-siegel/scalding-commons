package com.twitter.scalding.commons.scheme.scrooge

import com.twitter.elephantbird.mapreduce.io.BinaryConverter
import com.twitter.scrooge.{BinaryThriftStructSerializer, ThriftStruct, ThriftStructCodec}
import org.apache.thrift.TException

class ScroogeConverter[M <: ThriftStruct : Class] extends BinaryConverter[M] {

  private val serializer = new BinaryThriftStructSerializer[M] {
    override def codec = ScroogeConverter.codec[M]
  }

  override def fromBytes(buf: Array[Byte]): M = {
    try {
      serializer.fromBytes(buf)
    } catch {
      case exc: TException => null.asInstanceOf[M]
    }
  }

  override def toBytes(m: M): Array[Byte] = {
    try {
      serializer.toBytes(m)
    } catch {
      case exc: TException => null.asInstanceOf[Array[Byte]]
    }
  }

}

object ScroogeConverter {

  def codec[M <: ThriftStruct : Class] = {
    // This is a hack to get the scrooge codec for a given class.  The codec
    // is always implemented as the companion object for the class, so we look
    // it up by name.
    val companionClass = Class.forName(implicitly[Class[M]].getName + "$")
    val companionObject = companionClass.getField("MODULE$").get(null)
    companionObject.asInstanceOf[ThriftStructCodec[M]]
  }

}