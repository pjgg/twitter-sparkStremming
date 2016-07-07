package org.pjgg.sparkStreamming

import org.slf4j.{Logger, LoggerFactory}

trait LazyLogging {

  protected lazy val LOG: Logger = LoggerFactory.getLogger(getClass.getName)

}

trait StrictLogging {

  protected val LOG: Logger = LoggerFactory.getLogger(getClass.getName)

}