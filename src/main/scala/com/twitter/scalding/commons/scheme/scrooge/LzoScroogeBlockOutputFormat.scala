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
