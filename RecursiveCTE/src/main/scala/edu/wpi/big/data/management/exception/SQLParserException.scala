package edu.wpi.big.data.management.exception

final case class SQLParserException(private val message: String = "", private val cause: Throwable = None.orNull)
  extends Exception(message, cause)
