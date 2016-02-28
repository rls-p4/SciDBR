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
    x = merge(x,`by`[[1]])
    n = scidb_attributes(x)[length(scidb_attributes(x))]
    `by`[[1]] = n
  }

  b = `by`
  new_dim_name = make.names_(c(unlist(b),"row"))
  new_dim_name = new_dim_name[length(new_dim_name)]
  if(!all(b %in% c(scidb_attributes(x), dimensions(x), "")))
  {
# Check for numerically-specified coordinate axes and replace with dimension
# labels.
    for(k in 1:length(b))
    {
      if(is.numeric(b[[k]]))
      {
        b[[k]] = dimensions(x)[b[[k]]]
      }
    }
  }
  if(!all(b %in% c(scidb_attributes(x), dimensions(x), ""))) stop("Invalid attribute or dimension name in by")
  a = scidb_attributes(x) %in% b
  query = x@name
# Handle group by attributes with redimension. We don't use a redimension
# aggregate, however, because some of the other group by variables may already
# be dimensions.
  if(any(a))
  {
# First, we check to see if any of the attributes are not int64. In such cases,
# we use index_lookup to create a factorized version of the attribute to group
# by in place of the original specified attribute. This creates a new virtual
# array x with additional attributes.
    types = scidb_attributes(x)[a]
    nonint = scidb_types(x) != "int64" & a
    if(any(nonint))
    {
# Use index_lookup to factorize non-integer indices, creating new enumerated
# attributes to sort by. It's probably not a great idea to have too many.
      idx = which(nonint)
      oldatr = scidb_attributes(x)
      for(j in idx)
      {
        atr     = oldatr[j]
# Adjust the FUN expression to include the original attribute
# The gsub is a fix for github issue #61.
        FUN = sprintf("%s, min(%s) as %s", gsub("\\(\\)","(*)",FUN), atr, atr)
# Factorize atr
        x       = index_lookup(x,unique(sort(project(x,atr)),sort=FALSE),atr)
# Name the new attribute and sort by it instead of originally specified one.
        newname = paste(atr,"index",sep="_")
        newname = make.unique_(oldatr,newname)
        b[which(b==atr)] = newname
      }
    }

# Reset in case things changed above
    a = scidb_attributes(x) %in% b
    n = scidb_attributes(x)[a]
# XXX EXPERIMENTAL
# We estimate rational chunk sizes here.
    app = paste(paste("ApproxDC(",n,")",sep=""),collapse=",")
    aq = sprintf("aggregate(project(%s,%s),%s)",x@name,paste(n,collapse=","),app)
    acounts = iquery(aq,return=TRUE)  # acounts[2],acounts[3],...
    chunka = acounts[-1]
    dima = paste(paste(n,"=0:",.scidb_DIM_MAX,",",noE(chunka),",0",sep=""), collapse=",")
    D = paste(build_dim_schema(x,bracket=FALSE),dima,sep=",")
    S = build_attr_schema(x, I=!a)
    D = sprintf("[%s]",D)
    query = sprintf("redimension(%s,%s%s)",x@name,S,D)
  }
  along = paste(b,collapse=",")

  if(!missing(window))
  {
    query = sprintf("window(%s, %s, %s)",query,paste(noE(window),collapse=","),FUN)
  } else if(!missing(variable_window))
  {
    query = sprintf("variable_window(%s, %s, %s, %s)",query,along,paste(noE(variable_window),collapse=","),FUN)
  } else
  if(nchar(along)<1)
    query = sprintf("aggregate(%s, %s)", query, FUN)
  else
    query = sprintf("aggregate(%s, %s, %s)",query, FUN, along)
  .scidbeval(query, gc=TRUE,depend=list(x))
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
