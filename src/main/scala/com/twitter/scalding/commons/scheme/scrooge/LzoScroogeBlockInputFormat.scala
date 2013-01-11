package com.twitter.scalding.commons.scheme.scrooge

import com.twitter.elephantbird.mapreduce.input.{LzoInputFormat, LzoBinaryBlockRecordReader}
import com.twitter.elephantbird.mapreduce.io.{BinaryWritable, BinaryBlockReader}
import com.twitter.elephantbird.util.TypeRef
import com.twitter.scrooge.ThriftStruct
import java.io.InputStream
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.{TaskAttemptContext, InputSplit}

class LzoScroogeBlockInputFormat[M <: ThriftStruct]
  extends LzoInputFormat[LongWritable, BinaryWritable[M]] {

  override def createRecordReader(split: InputSplit, job: TaskAttemptContext) = {
    val className = job.getConfiguration.get("scalding.scrooge.class.for.LzoScroogeBlockInputFormat")
    implicit val tClass = job.getConfiguration.getClassByName(className).asInstanceOf[Class[M]]
    new LzoScroogeBlockRecordReader[M]
  }

}

class LzoScroogeBlockRecordReader[M <: ThriftStruct : Class]
  extends LzoBinaryBlockRecordReader[M, BinaryWritable[M]](
    LzoScroogeBlockRecordReader.typeRef[M],
    new ScroogeBlockReader(null),
    new ScroogeWritable[M]
  )

object LzoScroogeBlockRecordReader {
  def typeRef[M : Class] = new TypeRef[M](implicitly[Class[M]]) {}
}

class ScroogeBlockReader[M <: ThriftStruct : Class](in: InputStream)
  extends BinaryBlockReader[M](in, new ScroogeConverter[M])
