setGeneric("c")
setGeneric("head")
setGeneric("is.scidb", function(x) standardGeneric("is.scidb"))

#' @export
setMethod(c, signature(x="scidb"),
function(x, y)
{
  if(as.numeric(scidb_coordinate_bounds(x)$length) < as.numeric(.scidb_DIM_MAX))
  {
    s = sprintf("%s%s",build_attr_schema(x),build_dim_schema(x,newend=.scidb_DIM_MAX))
    x = redimension(x,s)
  }
  i = count(x) + as.numeric(scidb_coordinate_start(x)) - as.numeric(scidb_coordinate_start(y))
  j = make.unique_(y@attributes, "j")
  fun = sprintf("%s + %.0f", dimensions(y), i)
  s = sprintf("apply(%s, %s, %s)",y@name, j, fun)
  scma = sprintf("%s%s",build_attr_schema(y), build_dim_schema(x,newname=j))
  s = sprintf("redimension(%s, %s)",s, scma)
  s = sprintf("cast(%s, %s%s)", s,build_attr_schema(y), build_dim_schema(x))
  s = sprintf("merge(%s, %s)", x@name, s)
  .scidbeval(s, gc=TRUE, depend=list(x, y))
})

#' @export
setMethod("head", signature(x="scidb"),
function(x, n=6L, ...)
{
  iqdf(x, n)[,-1]
})

#' @export
setMethod('is.scidb', signature(x='scidb'),
  function(x) return(TRUE))
#' @export
setMethod('is.scidb', definition=function(x) return(FALSE))

#' @export
setMethod('print', signature(x='scidb'),
  function(x) {
    show(x)
  })

#' @export
#' @importFrom graphics hist
setMethod("hist",signature(x="scidb"), hist_scidb)

#' @export
setMethod('show', 'scidb',
  function(object) {
    .scidbstr(object)
  })

regrid_scidb = function(x, grid, expr)
  {
    if(missing(expr)) expr = paste(sprintf("max(%s)", x@attributes), collapse=",")
    if(is.function(expr))
    {
      expr = paste(as.character(as.list(match.call()$expr)), sprintf("(%s)", x@attributes), collapse=",")
    }
    query = sprintf("regrid(%s, %s, %s)",
               x@name, paste(noE(grid), collapse=","), expr)
    .scidbeval(query, eval=FALSE, gc=TRUE, depend=list(x))
  }
setGeneric("regrid", function(x, grid, expr) standardGeneric("regrid"))
#' SciDB regrid decimation operator
#' @aliases regrid
#' @param x SciDB array
#' @param grid a vector of grid sizes as long as \code{length(dimensions(x))}
#' @param expr optional aggregation function applied to every attribute on the grid, or a quoted SciDB aggregation expression
#' @note The default aggregation function used in case \code{expr} is left missing is \code{max}.
#' @examples
#' \dontrun{
#' x <- as.scidb(iris)
#' regrid(x, 10, min)
#' }
#' @importFrom stats aggregate
#' @return a \code{scidb} object
#' @export
setMethod("regrid", signature(x="scidb"), regrid_scidb)

xgrid_scidb = function(x, grid)
  {
    query = sprintf("xgrid(%s, %s)", x@name, paste(noE(grid), collapse=","))
    .scidbeval(query, eval=FALSE, gc=TRUE, depend=list(x))
  }
setGeneric("xgrid", function(x, grid, expr) standardGeneric("xgrid"))
#' SciDB xgrid prolongation operator
#' @aliases xgrid
#' @param x SciDB array
#' @param grid a vector of grid sizes as long as \code{length(dimensions(x))}
#' @examples
#' \dontrun{
#' x <- as.scidb(iris)
#' y <- regrid(x, 10, min)
#' z <- xgrid(y, 10)
#' }
#' @export
setMethod("xgrid", signature(x="scidb"), xgrid_scidb)


#' Aggregate a SciDB Array Grouped by a Subset of its Dimensions and/or Attributes
#'
#' Group the \code{scidb} array object \code{x} by dimensions
#' and/or attributes in the array.  applying the valid SciDB aggregation function
#' \code{FUN} expressed as a character string to the groups. Set eval to TRUE to
#' execute the aggregation and return a scidb object; set eval to FALSE to return
#' an unevaluated SciDB array promise, which is essentially a character string
#' describing the query that can be composed with other SciDB package functions.
#' 
#' If an R reduction function is speciied for \code{FUN}, it will be
#' transliterated to a SciDB aggregate.
#' 
#' The \code{by} argument must be a list of dimension names and/or attribute names
#' in the array \code{x} to group by, or a SciDB array reference object.  If
#' \code{by} is not specified and one of the \code{window} options is not
#' specified, then a grand aggregate is performed over all values in the array.
#' 
#' The argument \code{by} may be a list of dimension names and/or attributes of the
#' array \code{x}. Attributes that are not of type int64 will be `factorized` first
#' and replaced by enumerated int64 values that indicate each unique level (this
#' requires SciDB 13.6 or higher).
#' 
#' When \code{by} is a SciDB array it must contain one or more common dimensions
#' with \code{x}.  The two arrays will be joined (using SciDB
#' \code{cross_join(x,by)} and the resulting array will be grouped by the
#' attributes in the \code{by} array. This is similar to the usual R data.frame
#' aggregate method.
#' 
#' Perform moving window aggregates by specifying the optional \code{window} or
#' \code{variable_window} arguments. Use \code{window} to compute the aggregate
#' expression along a moving window specified along each coordinate axis as
#' \code{window=c(dimension_1_low, dim_1_high, dim_2_low,_dim_2_high, ...}.
#' Moving window aggregates along coordinates may be applied in multiple
#' dimensions.
#' 
#' Use \code{variable_window} to perform moving window aggregates over data
#' values in a single dimension specified by the \code{by} argument. See below
#' for examples. Moving window aggregates along data values are restricted
#' to a single array dimension.
#' @param x A \code{scidb} object.
#' @param by optional single character string or a list of array dimension and/or attribute names to group by;
#' or a \code{scidb} object to group by. Not required for \code{windowed} and grand aggregates--see details.
#' @param FUN a character string representing a SciDB aggregation expression or a reduction function.
#' @param window optional, if specified, perform a moving window aggregate along the specified coordinate windows--see details below.
#' @param variable_window optional, if specified, perform a moving window aggregate over successive data values along the
#' coordinate dimension axis specified by \code{by}--see details below.
#' @return a \code{scidb} object
#' @export
#' @examples
#' \dontrun{
#' # Create a copy of the iris data frame in a 1-d SciDB array named "iris."
#' # Note that SciDB attribute names will be changed to conform to SciDB
#' # naming convention.
#' x <- as.scidb(iris,name="iris")
#' 
#' # Compute averages of each variable grouped by Species
#' a <- aggregate(x, by="Species", FUN=mean)
#' 
#' # Aggregation by an auxillary vector (which in this example comes from
#' # an R data frame)--also note any valid SciDB aggregation expression may
#' # be used:
#' y <- as.scidb(data.frame(sample(1:4, 150, replace=TRUE)))
#' aggregate(x, by=y, FUN="avg(Petal_Width) as apw, min(Sepal_Length) as msl")[]
#' 
#' # Use the window argument to perform moving window aggregates along coordinate
#' # systems. You need to supply a window across all the array dimesions.
#' set.seed(1)
#' a <- matrix(rnorm(20), nrow=5)
#' A <- as.scidb(a)
#' # Compute a moving window aggregate only along the rows summing two rows at
#' # a time (returning result to R). The notation (0,1,0,0) means apply the
#' # aggregate over the current row (0) and (1) following row, and just over
#' # the current column (that is, a window size of one), returned in a data frame.
#' x <- aggregate(A, FUN=sum, window=c(0,1,0,0))[]
#' # Convert the returned data frame to a matrix:
#' X <- matrix(0, 5, 4)
#' X[as.matrix(x[,1:2] + 1)] <- x[,3]
#' X
#' # The above aggregate is equivalent to, for example:
#' apply(a, 2, function(x) x + c(x[-1], 0))
#' 
#' # Moving windows using the window= argument run along array coordinates.
#' # Moving windows using the variable_window= argument run along data values,
#' # skipping over empty array cells. The next example illustrates the
#' # difference.
#' 
#' # First, create an array with empty values:
#' B <- subset(A, val > 0)
#' # Here is what B looks like (in data frame form):
#' B[]
#' # Now, run a moving window aggregate along the rows with window just like
#' # the above example:
#' aggregate(B, FUN=sum, window=c(0,1,0,0))[]
#' # And now, a moving window along only the data values down the rows, note
#' # that we need to specify the dimension with by=:
#' aggregate(B, by="i", FUN=sum, variable_window=c(0,1))[]
#' }
setMethod("aggregate", signature(x="scidb"), aggregate_scidb)
