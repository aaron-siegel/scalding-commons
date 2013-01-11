/*
Copyright 2012 Twitter, Inc.

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

package com.twitter.scalding.commons.source

import com.google.protobuf.Message
import com.twitter.scalding._
import com.twitter.scalding.Dsl._
import com.twitter.scrooge.ThriftStruct
import java.io.Serializable
import org.apache.thrift.TBase

abstract class FixedPathLzoThrift[T <: TBase[_, _]: Manifest](path: String*)
  extends FixedPathSource(path: _*) with LzoThrift[T] {
  def column = manifest[T].erasure
}

abstract class FixedPathLzoScrooge[T <: ThriftStruct: Manifest](path: String)
  extends FixedPathSource(path) with LzoScrooge[T] {
  override val tClass = manifest[T].erasure.asInstanceOf[Class[T]]
}

abstract class FixedPathLzoProtobuf[T <: Message: Manifest](path: String)
  extends FixedPathSource(path) with LzoProtobuf[T] {
  def column = manifest[T].erasure
}
