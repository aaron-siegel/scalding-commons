/*
Copyright 2013 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
