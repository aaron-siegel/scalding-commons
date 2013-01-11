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

import cascading.flow.FlowProcess
import cascading.tap.Tap
import com.twitter.elephantbird.cascading2.scheme.LzoBinaryScheme
import com.twitter.elephantbird.mapred.input.DeprecatedInputFormatWrapper
import com.twitter.elephantbird.mapred.output.DeprecatedOutputFormatWrapper
import com.twitter.elephantbird.util.HadoopUtils
import com.twitter.scrooge.ThriftStruct
import org.apache.hadoop.mapred.{OutputCollector, RecordReader, JobConf}

class LzoScroogeScheme[M <: ThriftStruct](tClass: Class[M]) extends LzoBinaryScheme[M, ScroogeWritable[M]] {

  override def sourceConfInit(hfp: FlowProcess[JobConf], tap: Tap[JobConf, RecordReader[_,_], OutputCollector[_,_]], conf: JobConf) {
    HadoopUtils.setClassConf(conf, "scalding.scrooge.class.for.LzoScroogeBlockInputFormat", tClass)
    DeprecatedInputFormatWrapper.setInputFormat(classOf[LzoScroogeBlockInputFormat[M]], conf)
  }

  override def sinkConfInit(hfp: FlowProcess[JobConf], tap: Tap[JobConf, RecordReader[_,_], OutputCollector[_,_]], conf: JobConf) {
    HadoopUtils.setClassConf(conf, "scalding.scrooge.class.for.LzoScroogeBlockOutputFormat", tClass)
    DeprecatedOutputFormatWrapper.setOutputFormat(classOf[LzoScroogeBlockOutputFormat[M]], conf)
  }

  override protected def prepareBinaryWritable() = new ScroogeWritable[M]()(tClass)

}
