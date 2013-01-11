package com.twitter.scalding.commons.scheme.scrooge

import com.twitter.elephantbird.mapreduce.io.BinaryWritable
import com.twitter.scrooge.ThriftStruct

class ScroogeWritable[M <: ThriftStruct : Class]
  extends BinaryWritable[M](null.asInstanceOf[M], new ScroogeConverter[M]) {

  override def getConverterFor(tClass: Class[M]) = new ScroogeConverter[M]

}
