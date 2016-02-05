setGeneric("c")
setGeneric("head")
setGeneric("is.scidbdf", function(x) standardGeneric("is.scidbdf"))
setGeneric("unpack", unpack_scidb)

#' @export
setMethod(c, signature(x="scidbdf"),
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
setMethod("head", signature(x="scidbdf"),
function(x, n=6L, ...)
{
  iqdf(x, n)[,-c(1,2)]
})

#' @export
setMethod("tail", signature(x="scidbdf"),
function(x, n=6L, ...)
{
  ans = x[x[,1],]
  end = as.numeric(scidb_coordinate_bounds(ans)$end)
  start = max(0, end-n+1)
  ans[start:end,][]
})

#' @export
setMethod("Filter",signature(f="character",x="scidbdf"),
  function(f, x)
  {
    filter_scidb(x,f)
  })

#' @export
setMethod('is.scidbdf', signature(x='scidbdf'),
  function(x) return(TRUE))
#' @export
setMethod('is.scidbdf', definition=function(x) return(FALSE))

#' @export
setMethod('print', signature(x='scidbdf'),
  function(x) {
    show(x)
  })

#' @export
setMethod("hist",signature(x="scidbdf"), hist_scidb)

#' @export
setMethod('show', 'scidbdf',
  function(object) {
    v = ifelse(length(object@attributes) < 2, "variable", "variables")
    l = scidb_coordinate_bounds(object)$length
    if(as.numeric(l) > 4e18) l = "*"
    cat(sprintf("SciDB 1-D array: %s obs. of %d %s.\n", l,
        length(object@attributes),v))
  })

#setMethod("regrid", signature(x="scidbdf"),
#  function(x, grid, expr)
#  {
#    if(missing(expr)) expr = paste(sprintf("max(%s)",x@attributes),collapse=",")
#    query = sprintf("regrid(%s, %s, %s)",
#               x@name, paste(noE(grid),collapse=","), expr)
#    .scidbeval(query, eval=FALSE, gc=TRUE, depend=list(x))
#  })
#setMethod("xgrid", signature(x="scidbdf"),
#  function(x, grid)
#  {
#    query = sprintf("xgrid(%s, %s)", x@name, paste(noE(grid),collapse=","))
#    .scidbeval(query, eval=FALSE, gc=TRUE, depend=list(x))
#  })
setMethod("unpack",signature(x="scidbdf"),unpack_scidb)
#' @export
setMethod("aggregate", signature(x="scidbdf"), aggregate_scidb)

scidbdf_grand = function(x, op)
{
  query = sprintf("aggregate(%s, %s(%s) as %s)", x@name, op, x@attributes[1], x@attributes[1])
  iquery(query, `return`=TRUE)[,2]
}

# The following methods return data to R
#' @export
setMethod("sum", signature(x="scidbdf"),
function(x)
{
  scidbdf_grand(x, "sum")
})

#' @export
setMethod("median", signature(x="scidbdf"),
function(x)
{
  scidbdf_grand(x, "median")
})

#' @export
setMethod("mean", signature(x="scidbdf"),
function(x)
{
  scidbdf_grand(x, "avg")
})

#' @export
setMethod("min", signature(x="scidbdf"),
function(x)
{
  scidbdf_grand(x, "min")
})

#' @export
setMethod("max", signature(x="scidbdf"),
function(x)
{
  scidbdf_grand(x, "max")
})

#' @export
setMethod("sd", signature(x="scidbdf"),
function(x)
{
  scidbdf_grand(x, "stdev")
})

#' @export
setMethod("var", signature(x="scidbdf"),
function(x)
{
  scidbdf_grand(x, "var")
})

#' @export
log.scidbdf = function(x, base=exp(1))
{
  log_scidb(x,base) 
}

#' @export
setMethod("sin", signature(x="scidbdf"),
  function(x)
  {
    fn_scidb(x, "sin")
  })
#' @export
setMethod("cos", signature(x="scidbdf"),
  function(x)
  {
    fn_scidb(x, "cos")
  })
#' @export
setMethod("tan", signature(x="scidbdf"),
  function(x)
  {
    fn_scidb(x, "tan")
  })
#' @export
setMethod("asin", signature(x="scidbdf"),
  function(x)
  {
    fn_scidb(x, "asin")
  })
#' @export
setMethod("acos", signature(x="scidbdf"),
  function(x)
  {
    fn_scidb(x, "acos")
  })
#' @export
setMethod("atan", signature(x="scidbdf"),
  function(x)
  {
    fn_scidb(x, "atan")
  })
#' @export
setMethod("abs", signature(x="scidbdf"),
  function(x)
  {
    fn_scidb(x, "abs")
  })
#' @export
setMethod("sqrt", signature(x="scidbdf"),
  function(x)
  {
    fn_scidb(x, "sqrt")
  })
#' @export
setMethod("exp", signature(x="scidbdf"),
  function(x)
  {
    fn_scidb(x, "exp")
  })
