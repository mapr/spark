/* Copyright (c) 2015 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.db.spark.RDD

import com.mapr.db.spark.documentUtils.{BeanIterator, JavaBeanIterator, OJAIDocumentIterator}
import com.mapr.db.spark.impl.OJAIDocument
import java.util
import org.ojai.Document

sealed trait RDDTYPE[T] extends Serializable {
  def getValue(docs: java.util.Iterator[Document],
               beanClass: Class[T]): Iterator[T]
}

object RDDTYPE extends BaseRDDTYPE {
  implicit def defaultType: RDDTYPE[OJAIDocument] = new RDDTYPE[OJAIDocument] {
    def getValue(docs: java.util.Iterator[Document],
                 beanClass: Class[OJAIDocument]): Iterator[OJAIDocument] =
      new OJAIDocumentIterator(docs)
  }
}

trait BaseRDDTYPE {
  implicit def overridedefaulttype[T <: AnyRef]: RDDTYPE[T] = new RDDTYPE[T] {
    def getValue(doc: java.util.Iterator[Document],
                 beanClass: Class[T]): Iterator[T] =
      new BeanIterator(doc, beanClass)
  }

  def overrideJavaDefaultType[T <: java.lang.Object]: RDDTYPE[T] =
    new RDDTYPE[T] {
      override def getValue(doc: util.Iterator[Document],
                            beanClass: Class[T]): Iterator[T] =
        new JavaBeanIterator(doc, beanClass)
    }
}
