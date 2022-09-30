###
### @P4COPYRIGHT_SINCE:2022@
###

#'
#' @file generic_connection.R
#' @brief S3-style generic function layer for connection classes.
#'
#' @detail Each generic function "fn" in this file has concrete functions
#'   "fn.shim" and "fn.httpapi" defined in another file,
#'   which are dispatched to via UseMethod() in the generic function.
#'

#' Get the SciDB server's version.
#' @param db scidb connection object from \code{\link{scidbconnect}}
#' @return db with modifications reflecting the server version.
#'   In particular: attr(db, "connection")$scidb.version is the SciDB version,
#'   and class(db) has either "httpapi" or "shim" in its inheritance list,
#'   reflecting the interface that the connection uses.
GetServerVersion = function(db) 
{
  ## Dispatch to GetServerVersion.shim or GetServerVersion.httpapi.
  UseMethod("GetServerVersion")
}

#' Start a new connection/session.
#' The connection will automatically close when the connection object
#' goes out of scope.
#' @param db scidb connection object from \code{\link{scidbconnect}}
#' @return db with modifications reflecting the new connection.
#'   In particular: attr(db, "connection")$session is the session ID,
#'   and attr(db, "connection")$id is a unique connection ID.
Connect = function(db) 
{
  ## Dispatch to Connect.shim or Connect.httpapi.
  UseMethod("Connect")
}

#' Close the connection and session.
#' @param db scidb connection object from \code{\link{scidbconnect}}
Close = function(db)
{
  ## Dispatch to Close.shim or Close.httpapi.
  UseMethod("Close")
}

#' Run a query. Uses same interface as the iquery() function.
#' @see iquery()
Query = function(db, query, `return`=FALSE, binary=TRUE, arrow=FALSE, ...)
{
  ## Dispatch to Query.shim or Query.httpapi.
  UseMethod("Query")
}
