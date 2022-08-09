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

package org.apache.spark.editor

import org.apache.commons.lang3.StringUtils

import java.io.{File, FileOutputStream}
import scala.xml._


object HiveSiteEditor {
  private val hiveSitePath: String = {
    val sparkVersionSource = scala.io.Source.fromFile("/opt/mapr/spark/sparkversion")
    val sparkVersion = sparkVersionSource.getLines().mkString
    sparkVersionSource.close()
    s"/opt/mapr/spark/spark-${sparkVersion}/conf/hive-site.xml"
  }

  private val xml: Elem = XML.loadFile(hiveSitePath)
  private val property: NodeSeq = xml \ "property"
  private val properties = property.map(toProperty)
  private val xmlHeader = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?><configuration>\n"

  case class Property(name: String, value: String, description: String)

  def main(args: Array[String]): Unit = {
    if (args(0).equals("delete")) save(delete(args))
    else if (args(0).equals("create")) save(create(args))
    else if (args(0).equals("replace")) save(replace(args))
    else throw new IllegalArgumentException("Need to set operation name as firs argument: create | replace | delete")
  }

  // create name=value name1=value1 ...
  def create(args: Array[String]): Seq[Property] = {
    val inputProps = parsePropertiesFromArgs(args, "create")
    val replacedProps =  properties.map(p => {
      val i = inputProps.indexWhere(_.name.equals(p.name))
      if (i >= 0) Property(name = inputProps(i).name, value = inputProps(i).value, description = p.description) else p
    })
    val create = inputProps.map(np => {
      val i = replacedProps.indexWhere(_.name.equals(np.name))
      if (i == -1) Property(name = np.name, value = np.value, description = "") else Property("", "", "")
    }).filterNot(_.name.equals(""))
    replacedProps ++ create
  }

  // replace name=value name1=value1 ...
  def replace(args: Array[String]): Seq[Property] = {
    val inputProps = parsePropertiesFromArgs(args, "replace")
    properties.map(p => {
      val i = inputProps.indexWhere(_.name.equals(p.name))
      if (i >= 0) Property(name = inputProps(i).name, value = inputProps(i).value, description = p.description) else p
    })
  }

  // delete name name1 ...
  def delete(args: Array[String]): Seq[Property] = {
    property.filterNot(p => StringUtils.equalsAnyIgnoreCase((p \ "name").text, args: _*))
      .map(toProperty)
  }

  def save(properties: Seq[Property]): Unit = {
    val fos = new FileOutputStream(new File(hiveSitePath))
    Console.withOut(fos) {
      println(xmlHeader)
      properties.map(toXml).foreach(println)
      println("</configuration>")
    }
  }

  def parsePropertiesFromArgs(args: Array[String], methodName: String): Seq[Property] = {
    val argsMap = args.filterNot(_.equals(methodName)).map(_.split("="))
      .map(arr => arr(0) -> arr(1)).toMap
    argsMap.map(x => Property(x._1, x._2, "")).toSeq
  }

  def toProperty(node: Node): Property = {
    val name = (node \ "name").text
    val value = (node \ "value").text
    val description = (node \ "description").text
    Property(name, value, description)
  }

  def toXml(p: Property): Node = {
    <property>
      <name>{p.name}</name>
      <value>{p.value}</value>
      <description>{p.description}</description>
    </property>
  }
  
}
