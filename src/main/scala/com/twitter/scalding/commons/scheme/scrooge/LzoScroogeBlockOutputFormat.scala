package com.twitter.scalding.commons.scheme.scrooge

import com.twitter.elephantbird.mapreduce.io.BinaryBlockWriter
import com.twitter.elephantbird.mapreduce.output.{LzoBinaryBlockRecordWriter, LzoOutputFormat}
import com.twitter.scrooge.ThriftStruct
import java.io.OutputStream
import org.apache.hadoop.mapreduce.TaskAttemptContext

class LzoScroogeBlockOutputFormat[M <: ThriftStruct]
  extends LzoOutputFormat[M, ScroogeWritable[M]] {

  override def getRecordWriter(job: TaskAttemptContext) = {
    val className = job.getConfiguration.get("scalding.scrooge.class.for.LzoScroogeBlockOutputFormat")
    implicit val tClass = job.getConfiguration.getClassByName(className).asInstanceOf[Class[M]]
    new LzoBinaryBlockRecordWriter(new ScroogeBlockWriter(getOutputStream(job)))
  }

}

class ScroogeBlockWriter[M <: ThriftStruct : Class](out: OutputStream)
  extends BinaryBlockWriter[M](out, implicitly[Class[M]], new ScroogeConverter[M], 100)
