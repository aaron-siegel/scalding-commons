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

import com.twitter.elephantbird.mapreduce.io.BinaryWritable
import com.twitter.scrooge.ThriftStruct

class ScroogeWritable[M <: ThriftStruct : Class]
  extends BinaryWritable[M](null.asInstanceOf[M], new ScroogeConverter[M]) {

  override def getConverterFor(tClass: Class[M]) = new ScroogeConverter[M]

}