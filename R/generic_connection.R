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

#' Unpack and return a SciDB query expression as a data frame
#' @param db scidb database connection object
#' @param query_or_scidb A SciDB query expression or scidb object
#' @param binary optional logical value. If \code{FALSE} use text transfer, 
#'    otherwise binary transfer. Defaults to \code{TRUE}.
#' @param buffer_size optional integer. Initial parse buffer size in bytes, 
#'    adaptively resized as needed. Larger buffers can be faster but consume
#'    more memory. Default size is determined by the connection implementation.
#' @param only_attributes optional logical value. \code{TRUE} means
#'    don't retrieve dimension coordinates, only return attribute values.
#'    Logically defaults to \code{FALSE} (but the default is actually NULL
#'      because shim needs to set it to TRUE when the query result is a 
#'      SciDB dataframe)
#' @param schema optional result schema string, only applies when \code{query} 
#'    is not a SciDB object. Supplying this avoids one extra metadata query to
#'    determine result schema. Defaults to \code{schema(query)}.
#' @importFrom curl new_handle handle_setheaders handle_setopt 
#'    curl_fetch_memory handle_setform form_file
#' @importFrom data.table  data.table
#' @import bit64
BinaryQuery = function(db, query_or_scidb, 
                       binary=TRUE,
                       buffer_size=NULL,     # implementation should decide
                       only_attributes=NULL, # shim implementation needs to see
                                             #    if query result is a dataframe
                       schema=NULL, 
                       ...)
{
  UseMethod("BinaryQuery")
}
