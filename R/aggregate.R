aggregate_scidb = function(x, by, FUN, window, variable_window)
{
  if(missing(`by`))
  {
    `by`=""
  }
  if(!is.list(`by`)) `by`=list(`by`)
# Check for common function names and map to SciDB expressions
  if(is.function(FUN))
  {
    M = match.call()
    fn = .scidbfun(FUN)
    cb = unlist(by[unlist(lapply(by,is.character))]) # may be empty
    if(is.null(fn))
      stop("Aggregate requires a valid SciDB aggregate function")
    FUN = paste(paste(fn,"(",setdiff(scidb_attributes(x),cb),")",sep=""),collapse=",")
  }

# XXX Why limit this to the first `by` element?
  if(class(`by`[[1]]) %in% "scidb")
  {
# We are grouping by attributes in another SciDB array `by`. We assume that
# x and by have conformable dimensions to join along!
    x = merge(x, `by`[[1]])
    n = scidb_attributes(x)[length(scidb_attributes(x))]
    `by`[[1]] = n
  }

  b = `by`
  if(!all(b %in% c(scidb_attributes(x), dimensions(x), "")))
  {
# Check for numerically-specified coordinate axes and replace with dimension
# or attribute labels.
    for(k in 1:length(b))
    {
      if(is.numeric(b[[k]]))
      {
        b[[k]] = names(x)[b[[k]]]
      }
    }
  }
  if(!all(b %in% c(scidb_attributes(x), dimensions(x), ""))) stop("Invalid attribute or dimension name in by")
  a = scidb_attributes(x) %in% b
  query = x@name
# Handle group by attributes with grouped_aggregate, not including window and
# variable_window aggregations.
  aggop = "aggregate"
  if(any(a)) aggop = "grouped_aggregate"
  along = paste(b, collapse=",")

  if(!missing(window))
  {
    query = sprintf("window(%s, %s, %s)", query, paste(noE(window), collapse=","), FUN)
  } else if(!missing(variable_window))
  {
    query = sprintf("variable_window(%s, %s, %s, %s)",query, along, paste(noE(variable_window), collapse=","), FUN)
  } else
  if(nchar(along) < 1)
    query = sprintf("%s(%s, %s)", aggop, query, FUN)
  else
    query = sprintf("%s(%s, %s, %s)", aggop, query, FUN, along)
  .scidbeval(query, gc=TRUE, depend=list(x))
}

#' SciDB Cumulative Aggregation
#' Use \code{cumulate} function to compute running operations along data,
#' for example cumulative sums.
#' @param x SciDB array
#' @param expression any valid SciDB aggregate expression, expressed as a character string
#' @param dimension optional array dimension name (character) or 1-based dimension index to run along
#'        (default is to use the first dimension)
#' @return a \code{scidb} object
#' @examples
#' \dontrun{
#' x <- as.scidb(iris)
#' y <- cumulate(x, "sum(Petal_Width)")
#' }
#' @export
cumulate = function(x, expression, dimension)
{
  if(missing(dimension)) dimension = dimensions(x)[[1]]
  if(is.numeric(dimension)) dimension = dimensions(x)[dimension]
  query = sprintf("cumulate(%s, %s, %s)",x@name,expression,dimension)
  .scidbeval(query, depend=list(x))
}
