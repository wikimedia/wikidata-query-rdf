/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wikidata.query.rdf

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.{createTypeInformation, DataStream, StreamExecutionEnvironment}
import org.slf4j.{Logger, LoggerFactory}

object LetterCountJob {

  private val LOG: Logger = LoggerFactory.getLogger(LetterCountJob.getClass)
  def process(source: DataStream[String], sink: SinkFunction[(String, Int)]): Unit = {
    source
      .map(s => (s, s.length()))
      .addSink(sink)
  }

  def main(args: Array[String]) {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment


    val strings: Seq[String] = Seq("the", "quick", "brown", "fox", "jumps", "over", "a", "lazy", "dog")
    val source = env.fromCollection(strings)

    val sink = new SinkFunction[(String, Int)] {
      override def invoke(value: (String, Int)): Unit = LOG.info(s"Length of ${value._1} is ${value._2}")
    }

    LetterCountJob.process(source, sink)

    // execute program
    env.execute("Letter Count Job")
  }
}
