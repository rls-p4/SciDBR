#' Upload R data to SciDB
#' Data are uploaded as TSV.
#' @param X an R data frame, raw value, Matrix, matrix, or vector object
#' @param name a SciDB array name to use
#' @param start starting SciDB integer coordinate index
#' @param gc set to FALSE to disconnect the SciDB array from R's garbage collector
#' @param ... other options, see \code{\link{df2scidb}}
#' @return A \code{scidb} object
#' @export
as.scidb = function(X,
                    name=tmpnam(),
                    start,
                    gc=TRUE, ...)
{
  if(inherits(X, "raw"))
  {
    return(raw2scidb(X, name=name, gc=gc,...))
  }
  if(inherits(X, "data.frame"))
  {
    return(df2scidb(X, name=name, gc=gc, start=start, ...))
  }
  if(inherits(X, "dgCMatrix"))
  {
    return(.Matrix2scidb(X, name=name, start=start, gc=gc, ...))
  }
  return(matvec2scidb(X, name=name, start=start, gc=gc, ...))
}
