setGeneric("c")
setGeneric("head")
setGeneric("is.scidb", function(x) standardGeneric("is.scidb"))
setGeneric("unpack", unpack_scidb)

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
  iqdf(x, n)[,-c(1,2)]
})

#' @export
setMethod("tail", signature(x="scidb"),
function(x, n=6L, ...)
{
  ans = x[x[,1],]
  end = as.numeric(scidb_coordinate_bounds(ans)$end)
  start = max(0, end-n+1)
  ans[start:end,][]
})

#' @export
setMethod("Filter",signature(f="character",x="scidb"),
  function(f, x)
  {
    filter_scidb(x,f)
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
setMethod("hist",signature(x="scidb"), hist_scidb)

#' @export
setMethod('show', 'scidb',
  function(object) {
    .scidbstr(object)
  })

#setMethod("regrid", signature(x="scidb"),
#  function(x, grid, expr)
#  {
#    if(missing(expr)) expr = paste(sprintf("max(%s)",x@attributes),collapse=",")
#    query = sprintf("regrid(%s, %s, %s)",
#               x@name, paste(noE(grid),collapse=","), expr)
#    .scidbeval(query, eval=FALSE, gc=TRUE, depend=list(x))
#  })
#setMethod("xgrid", signature(x="scidb"),
#  function(x, grid)
#  {
#    query = sprintf("xgrid(%s, %s)", x@name, paste(noE(grid),collapse=","))
#    .scidbeval(query, eval=FALSE, gc=TRUE, depend=list(x))
#  })
setMethod("unpack",signature(x="scidb"),unpack_scidb)
#' @export
setMethod("aggregate", signature(x="scidb"), aggregate_scidb)

scidb_grand = function(x, op)
{
  query = sprintf("aggregate(%s, %s(%s) as %s)", x@name, op, x@attributes[1], x@attributes[1])
  iquery(query, `return`=TRUE)[,2]
}

# The following methods return data to R
#' @export
setMethod("sum", signature(x="scidb"),
function(x)
{
  scidb_grand(x, "sum")
})

#' @export
setMethod("median", signature(x="scidb"),
function(x)
{
  scidb_grand(x, "median")
})

#' @export
setMethod("mean", signature(x="scidb"),
function(x)
{
  scidb_grand(x, "avg")
})

#' @export
setMethod("min", signature(x="scidb"),
function(x)
{
  scidb_grand(x, "min")
})

#' @export
setMethod("max", signature(x="scidb"),
function(x)
{
  scidb_grand(x, "max")
})

#' @export
setMethod("sd", signature(x="scidb"),
function(x)
{
  scidb_grand(x, "stdev")
})

#' @export
setMethod("var", signature(x="scidb"),
function(x)
{
  scidb_grand(x, "var")
})

#' @export
log.scidb = function(x, base=exp(1))
{
  log_scidb(x,base) 
}

#' @export
setMethod("sin", signature(x="scidb"),
  function(x)
  {
    fn_scidb(x, "sin")
  })
#' @export
setMethod("cos", signature(x="scidb"),
  function(x)
  {
    fn_scidb(x, "cos")
  })
#' @export
setMethod("tan", signature(x="scidb"),
  function(x)
  {
    fn_scidb(x, "tan")
  })
#' @export
setMethod("asin", signature(x="scidb"),
  function(x)
  {
    fn_scidb(x, "asin")
  })
#' @export
setMethod("acos", signature(x="scidb"),
  function(x)
  {
    fn_scidb(x, "acos")
  })
#' @export
setMethod("atan", signature(x="scidb"),
  function(x)
  {
    fn_scidb(x, "atan")
  })
#' @export
setMethod("abs", signature(x="scidb"),
  function(x)
  {
    fn_scidb(x, "abs")
  })
#' @export
setMethod("sqrt", signature(x="scidb"),
  function(x)
  {
    fn_scidb(x, "sqrt")
  })
#' @export
setMethod("exp", signature(x="scidb"),
  function(x)
  {
    fn_scidb(x, "exp")
  })
