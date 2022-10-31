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
GetServerVersion <- function(db) 
{
  ## Dispatch to GetServerVersion.shim or GetServerVersion.httpapi.
  UseMethod("GetServerVersion")
}

#' Start a new connection/session.
#' When this function finishes:
#'   - attr(db, "connection")$session is the session ID
#'   - attr(db, "connection")$id is a unique connection ID
#' The connection will automatically close when the connection object
#' goes out of scope.
#' @param db scidb connection object from \code{\link{scidbconnect}}
NewSession <- function(db, ...) 
{
  ## Dispatch to NewSession.shim or NewSession.httpapi.
  UseMethod("NewSession")
}

#' Make sure that we are connected to an active session in httpapi or shim,
#' creating a a session if we don't have one already.
#' @param db scidb connection object from \code{\link{scidbconnect}}
EnsureSession <- function(db, ...)
{
  ## Dispatch to EnsureSession.shim or EnsureSession.httpapi.
  UseMethod("EnsureSession")
}

#' If a session's authentication cookie times out, call this function to
#' supply the password again to resume the existing session.
#' @param db scidb connection object from \code{\link{scidbconnect}}
#' @param password the password to use for reauthenticating
#' @param defer if TRUE, the actual reauthentication is deferred until the
#'   next query is executed. This might delay feedback about whether the
#'   authentication succeeded or not.
Reauthenticate <- function(db, password, defer=FALSE)
{
  ## Dispatch to Reauthenticate.httpapi or Reauthenticate.shim
  UseMethod("Reauthenticate")
}

#' Close the connection and session.
#' @param db scidb connection object from \code{\link{scidbconnect}}
Close <- function(db)
{
  ## Dispatch to Close.shim or Close.httpapi.
  UseMethod("Close")
}

#' Execute an AFL command that is expected not to return any data.
Execute <- function(db, query_or_scidb, ...)
{
  ## Dispatch to Execute.shim or Execute.httpapi.
  UseMethod("Execute")
}

#' Run a query. Uses same interface as the iquery() function.
#' @see iquery()
Query <- function(db, query_or_scidb, 
                  `return`=FALSE, binary=TRUE, arrow=FALSE, ...)
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
BinaryQuery <- function(db, query_or_scidb, 
                        binary=TRUE,
                        buffer_size=NULL,     # implementation should decide
                        only_attributes=NULL, # shim implementation needs to see
                                              #  if query result is a dataframe
                        schema=NULL, 
                        ...)
{
  ## Dispatch to BinaryQuery.shim or BinaryQuery.httpapi
  UseMethod("BinaryQuery")
}

#' Upload R data and store() it into a SciDB array.
#' Return a scidb object wrapping the array.
#' @param db a scidb database connection returned from \code{\link{scidbconnect}}
#' @param payload an R data frame, raw value, Matrix, matrix, or vector object
#' @param name a SciDB array name to store the payload into, or NULL to 
#'    generate a unique name
#' @param start starting coordinate index, or NULL to start at zero on
#'    every dimension. Does not apply to data frames.
#' @param gc if TRUE, the SciDB array will be removed when the return value
#'    gets garbage-collected. Set to FALSE to disconnect the SciDB array 
#'    from R's garbage collector, i.e. to persist the SciDB array beyond 
#'    the lifetime of this session.
#' @param temp (boolean) make the SciDB array a temporary array
#'    (only lasting for the lifetime of the session)
#' @param ... other options, see each subclass implementation
#' @note Supported R objects include data frames, scalars, vectors, dense matrices,
#' and double-precision sparse matrices of class CsparseMatrix. Supported R scalar
#' types and their resulting SciDB types are:
#'  \itemize{
#'  \item{integer   -> }{int32}
#'  \item{logical   -> }{int32}
#'  \item{character -> }{string}
#'  \item{double    -> }{double}
#'  \item{integer64 -> }{int64}
#'  \item{raw       -> }{binary}
#'  \item{Date      -> }{datetime}
#' }
#' R factor values are converted to their corresponding character levels.
#' @seealso as.scidb
#' @return A \code{scidb} object
#' @export
Upload <- function(db, payload, name=NULL, gc=TRUE, temp=FALSE, ...)
{
  ## Dispatch to Upload.shim or Upload.httpapi
  UseMethod("Upload")
}

URI <- function(db_or_conn, resource="", args=list())
{
  ## Dispatch to URI.shim, URI.httpapi, or URI.default
  UseMethod("URI")
}
