/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.classpath

import java.io.File

import org.apache.tools.ant.DirectoryScanner

object ClasspathFilter {
  val scanner = new DirectoryScanner()
  scanner.setCaseSensitive(false)
  scanner.setIncludes(Array("**/*.jar"))


  def main(args: Array[String]): Unit = {
    val classpath = resolveClasspath(args(0).split(":")).toSet
    val blacklist = scala.io.Source.fromFile(new File(args(1))).mkString

    val filteredClasspath =
        classpath.map(new File(_))
        .filter { file =>
          file.exists() && !blacklist.contains(file.getAbsolutePath)
        }.mkString(":")

    print(filteredClasspath)
  }

  def resolveClasspath(classpath: Array[String]): Array[String] = {
    classpath.flatMap(path => {
      if (path.endsWith("/*") && path.startsWith("/")) {
        scanner.setBasedir(path.dropRight(1))
        scanner.scan()
        scanner.getIncludedFiles.map(jar => path.dropRight(1) + jar)
      } else Array(path)
    })
  }
}
