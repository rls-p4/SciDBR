# Image method from Alex P.
#' Display a SciDB array as an image
#'
#' Display a 2-d SciDB matrix as an image, downsampling the image in the database to
#' the specified resolution.
#' @param x \code{scidb} array object
#' @param grid  desired image resolution
#' @param op downsampling aggregation operator
#' @param na replace missing values with this value
#' @param ... additional arguments for the generic \code{image} method
#' @return An R matrix with the image values and the side-effect of displaying the image
#' plot of the matrix, unless \code{plot=FALSE} is specified in the optional arguments.
#' @importFrom methods setMethod
#' @importFrom graphics image axis
#' @export
setMethod("image", signature(x="scidb"),
          function(x, grid=c(500,500), op="sum", na=0, ...)
          {
            df2xyvm = function(x, na=0)
            {
              x[is.na(x[,3]),3] = na
              min_m = min(x[,1])
              min_n = min(x[,2])
              m = max(x[,1]) - min_m + 1
              n = max(x[,2]) - min_n + 1
              B = matrix(0,m,n)
              B[x[,1]-min_m + (x[,2]-min_n)*m  + 1] = x[,3]
              B
            }
            if(length(scidb_coordinate_start(x))!=2) stop("Sorry, array must be two-dimensional")
            if(length(grid)!=2) stop("The grid parameter must contain two values")
            if(any(is.infinite(as.numeric(scidb_coordinate_bounds(x)$length)))) stop("The array must be bounded")
            if(length(length(scidb_attributes(x)))!=1) stop("The array must have one attribute")
            el = list(...)
            if("plot" %in% names(el))
            {
              plot = as.logical(el$plot)
            }
            else plot=TRUE
            blocks = as.numeric(scidb_coordinate_bounds(x)$length)
            blocks = blocks/grid
            if(any(blocks<1)) blocks[which(blocks<1)] = 1
            attribute = scidb_attributes(x)[1]
            query = sprintf("regrid(%s,%.0f,%.0f,%s(%s))",x@name,blocks[1],blocks[2],op, attribute)
            A = iquery(query,return=TRUE,n=Inf)
            B = df2xyvm(A, na)
            if(!plot) return (B)
            xlbl=(1:ncol(B))*blocks[2]
            xat=seq(from=0,to=1,length.out=ncol(B))
            ylbl=(1:nrow(B))*blocks[1]
            yat=seq(from=0,to=1,length.out=nrow(B))
            image(B,xaxt='n',yaxt='n', ...)
            axis(side=1,at=xat,labels=xlbl)
            axis(side=2,at=yat,labels=ylbl)
            B
          })


#' SciDB array statistics
#'
#' Display array storage statistics
#' @param scidb_array either a character name of a stored array or a \code{scidb} array object
#' @param per_instance set to \code{TRUE} to break down storage stats by SciDB instance
#' @param per_attribute set to \code{TRUE} to break down stats by attribute
#' @return A data frame containinf the statistics
#' @export
array_stats = function (scidb_array, per_instance=FALSE, per_attribute=FALSE) 
{
  if (class(scidb_array) == "character") 
  {
    scidb_array = scidb(scidb_array)
  }
  if (!(class(scidb_array) %in% c("scidb", "scidbdf")))
  {
    stop("Invalid SciDB object")
  }
  result = iquery(sprintf("summarize(%s, 'per_instance=%i', 'per_attribute=%i')", 
                          scidb_array@name,
                          as.numeric(per_instance),
                          as.numeric(per_attribute)), return=TRUE)
  return(result)
}


#' Fast SciDB way to find unique elements
#'
#' A fast SciDB function for finding the unique elements among a set of attributes
#' in a SciDB array. This function uses the SciDB \code{\link{aggregate}} function.
#' @param scidb_array a \code{scidb} array reference object, or a character string naming a stored SciDB array
#' @param attributes a list or character vector of SciDB array attributes
#' @export
unordered_uniq = function(scidb_array, attributes)
{
  result = aggregate(scidb_array, by=attributes, FUN="count(*)")
  result = unpack(result)
  project(result, attributes)
}

#' SciDB dense matrix multiply
#' @param X a \code{scidb} array object
#' @param Y a \code{scidb} array object
#' @param Z (optional) \code{scidb} array object
#' @return a \code{scidb} array object corresponding to \code{X \%*\% Y + Z}
#' @export
gemm = function(X, Y, Z)
{
  if(missing(Z))
  {
    Z = scidb(sprintf("build(<val:double> [x=%s:%s,%s,0, y=%s:%s,%s,0], 0)", 
                      scidb_coordinate_start(X)[1],
                      scidb_coordinate_end(X)[1],
                      scidb_coordinate_chunksize(X)[1],
                      scidb_coordinate_start(Y)[2],
                      scidb_coordinate_end(Y)[2],
                      scidb_coordinate_chunksize(Y)[2]
    ))
  }
  result = scidb(sprintf("gemm(%s, %s, %s)", X@name, Y@name, Z@name))
  return(result)
}

#' SciDB dense matrix SVD
#' @param X a \code{scidb} array object
#' @param type a character vector either "left", "values" or "right" to return the
#' left singular vectors, singular values, or right singular vectors, respectively.
#' @return a \code{scidb} reference object containing the computed result
#' @export
gesvd = function(X, type=c("left", "values", "right"))
{
  type = match.arg(type)
  scidb(sprintf("gesvd(%s, '%s')", X@name, type))
}

transpose = function(X)
{
  result = scidb(sprintf("transpose(%s)", X@name))
  return(result)
}


#' Head-like SciDB array inspection
#'
#' Return the first part of an unpacked SciDB array as a data frame.
#' @param x SciDB array
#' @param n maximum number of rows to return
#' @param prob sampling probability
#' @note Setting the sampling probability low for huge arrays will improve performance.
#' @seealso \code{\link{head}}
#' @export
iqdf = function( x, n = 6L, prob = 1)
{
  result = x
  if ( class(result) == "character")
  {
    result = scidb(result)
  }
  got_limit = any(grepl("^limit$", .scidbenv$ops$name))
  if(is.infinite(n)) return(result[])
  if(got_limit)
  {
    return(iquery(sprintf("limit(%s, %.0f)", result@name, n), return=TRUE))
  }
  if ( prob < 1 )
  {
    result = bernoulli(result, prob)
  }
  result = unpack(result)
  if ( n > 0 && is.finite(n))
  {
    filter = sprintf("%s < %s", dimensions(result), noE(n))
    result = subset(result, filter)
  }
  result[]
}


# factor_scidb and levels_scidb define an experimental new hybrid class of
# R factors with levels from a SciDB indexing array. They're intended to make
# merge (joins) and redimension work better. See the doc for examples.
factor_scidb = function(x, levels)
{
  if(!(is.vector(x) ||  is.factor(x))) stop("x must be an R factor or vector object")
  if(!any(class(levels) %in% "scidb")) stop("levels must be an object of class scidb")

  if(!is.factor(x)) x = factor(x)
  l = index_lookup(as.scidb(levels(x)), levels)
  class(l) = "scidb"  # just make sure...
  l = l[]
  attr(x, "scidb_levels") = l[,2]
  attr(x, "scidb_index") = levels
  attr(x, "class") = c(attr(x, "class"), "scidb_factor")
  x
}

levels_scidb = function(x)
{
  if(!("scidb_factor" %in% class(x))) stop("x must be a scidb_factor object")
  attr(x, "scidb_levels")
}

#' Unbound SciDB array dimensions
#'
#' This function sets each dimension upper bound to '*' (that is, unspecified)
#' @param x a \code{scidb} object
#' @return a \code{scidb} object with unbounded dimension
#' @export
unbound = function(x)
{
  new_lengths = rep("*", length(dimensions(x)))
  new_dims = build_dim_schema(x, newlen=new_lengths)
  schema = sprintf("%s%s", build_attr_schema(x), new_dims)
  redimension(x, schema=schema)
}

range_scidb = function(x)
{
  a = scidb_attributes(x)[1]
  FUN = sprintf("min(%s) as min, max(%s) as max",a,a)
  aggregate(x, FUN=FUN)
}

bernoulli = function (x, prob , seed=sample(2^32 - 1 - 2^31, 1))
{
  if ( prob <= 0 || prob > 1 )
  {
    stop("Invalid prob value")
  }
  query = sprintf("bernoulli(%s, %.16f, %d)", x@name, prob, seed)
  return (scidb(query))
}

rank_scidb = function(x, na.last=TRUE, ties.method = c("average", "first", "random", "max", "min"))
{
  if(!is.scidb(x)) stop("x must be a scidb vector object")
  if(length(dim(x)) > 1) stop("x must be a scidb vector object")
  attribute = scidb_attributes(x)[1]
  dimension = ""
  ties.method = match.arg(ties.method)
  op = ifelse(ties.method == "average", "avg_rank", "rank")
  query = sprintf("%s(%s,%s%s)", op, x@name, attribute, dimension)
  .scidbeval(query, depend=list(x), eval=TRUE)
}

kmeans_scidb = function(x, centers, iter.max=30, nstart=1,
  algorithm="Lloyd")
{
  if(length(dim(x))!=2) stop("x must be a matrix")
  if(nstart!=1 || algorithm!="Lloyd") stop("This version limited to Lloyd's method with nstart=1")
# If we have a recent enough SciDB version, use temp arrays.
  temp = compare_versions(options("scidb.version")[[1]],14.8)
  if(!is.scidb(x)) stop("x must be a scidb object")
  x = project(x, scidb_attributes(x)[1])
  x = attribute_rename(x,new="val")
  x = dimension_rename(x,new=c("i","j"))
  expr = sprintf("random() %% %d", centers)
  group = scidbeval(build(expr, nrow(x), names=c("group","i"), type="int64"), temp=temp)
  dist_name = NULL
  k = centers
  diff_name = NULL
  for(iter in 1:iter.max)
  {
    centers = aggregate(x, by=list(group, "j"), FUN=mean)
    dist = scidbeval(
             aggregate(
               bind(
                 merge(x, centers, by="j"),
                   "dist", "(val - val_avg)*(val - val_avg)"
               ),
               by=list("i","group"), FUN="sum(dist) as dist")
            ,temp=temp, name=dist_name)
    dist_name = dist@name
    oldgroup = group
    group = scidbeval(redimension(
               Filter("dist = min",
                 merge(dist,
                       aggregate(dist,by="i", FUN="min(dist) as min"),
                       by="i")
               ),group), temp=temp)
# This is a too expensive operation, improve...
#    d = scidbeval(oldgroup - group, temp=TRUE, name=diff_name)
# Faster:
    d = project(bind(merge(oldgroup, group), "v", "group - group_1"), "v")
    diff_name = d@name
    if(sum(abs(d))[] < 1) break
  }
  if(iter==iter.max) warning("Reached maximum # iterations")
  list(cluster = group,
       centers = centers[0:(k-1),],
       iter    = iter)
}


# distance function SLOOOOOW!
dist_scidb = function(x, method=c("euclidean", "manhattan", "maximum"))
{
  if(length(dim(x))!=2) stop("dist requires a numeric matrix")
  method = match.arg(method)
# This should be faster for large problems, but only handles Euclidean
# distance...
#  u = apply(x*x,1,sum) %*% matrix(1.0,1,nrow(x))
#  ans = sqrt(abs(u + t(u) - 2 * x %*% t(x)))

# Faster, but not so natural. But it has the advantage that it can
# compute many different distance metrics.
  M     = merge(x,t(x),by.x=2, by.y=1)
  b     = scidb_attributes(M)[1]
  a     = make.unique_(scidb_attributes(M), "_")
  if(method=="euclidean")
  {
    dexpr = sprintf("pow(%s,2)",paste(scidb_attributes(M),collapse="-"))
    sexpr = sprintf("sum(%s) as %s",a,a)
    pexpr = sprintf("pow(%s,0.5)",a)
    M     = aggregate(bind(M, a, dexpr), by=list(1,3), FUN=sexpr)
    M     = subset(M, paste(dimensions(M),collapse=">"))
    M     = project(bind(M, b, pexpr),2)
  }
  if(method=="manhattan")
  {
    dexpr = sprintf("abs(%s)",paste(scidb_attributes(M),collapse="-"))
    sexpr = sprintf("sum(%s) as %s",a,b)
    M     = aggregate(bind(M, a, dexpr), by=list(1,3), FUN=sexpr)
    M     = subset(M, paste(dimensions(M),collapse=">"))
  }
  if(method=="maximum")
  {
    m = scidb_attributes(M)
    dexpr = sprintf("abs(%s)",paste(scidb_attributes(M),collapse="-"))
    sexpr = sprintf("max(%s) as %s",a,b)
    M     = aggregate(bind(M, a, dexpr), by=list(1,3), FUN=sexpr)
    M     = subset(M, paste(dimensions(M),collapse=">"))
  }
  M
}


# Several nice functions contributed by Alex Poliakov follow...

# Return TRUE if array1 has the same dimensions, same attributes and types and
# same data at the same coordinates False otherwise
all.equal.scidb = function ( target, current , ...)
{
  all.equal.scidb( target, current )
}

all.equal.scidb = function ( target, current , ...)
{
  array1 = target
  array2 = current
  if ( length(scidb_attributes(array1)) != length(scidb_attributes(array2)) )
  {
    return (FALSE)
  }
  if ( !all(scidb_types(array1) == scidb_types(array2) ))
  {
    return (FALSE)
  }
  a1dims = dimensions(array1)
  a2dims = dimensions(array2)
  if ( length(a1dims) != length(a2dims) )
  {
    return (FALSE)
  }
  a1count = count(array1)
  a2count = count(array2)
  if ( a1count != a2count )
  {
    return (FALSE)
  }
  array1 = attribute_rename(array1, new=sprintf("a_%s",scidb_attributes(array1)))
  array2 = attribute_rename(array2, new=sprintf("b_%s",scidb_attributes(array2)))

  join = merge(array1, array2, by.x=dimensions(array1), by.y=dimensions(array2))
  jcount = tryCatch(count(join), error=function(e) {return(FALSE)})
  if ( jcount != a1count)
  {
    return (FALSE)
  }
  filter_expr = paste( sprintf("%s <> %s", scidb_attributes(array1),scidb_attributes(array2)), collapse = " or ")
  jcount = count (subset(join,filter_expr))
  if ( jcount != 0)
  {
    return (FALSE)
  }
  return(TRUE)
}

#' Antijoin
#'
#' Given two arrays of same dimensionality, return any coordinates that do NOT
#' join. For all coordinates, the single attribute shall equal to 1 if those
#' coordinates exist in array1, or 2 if those coordinates exist in array2.
#' @param array1 SciDB array
#' @param array2 SciDB array
#' @return A single-attribute SciDB array
#' equal to 1 where the corresponding coordinates
#' exist in array1, or 2 if those coordinates exist in array2
#' @export
antijoin = function(array1, array2)
{
  a1dims = dimensions(array1)
  a2dims = dimensions(array2)
  if ( length(a1dims) != length(a2dims) )
  {
    stop("Incompatible dimensions")
  }
  a1count = count(array1)
  a2count = count(array2)
  join = merge(array1,array2)
  jcount = count(join)
  if(jcount == a1count && jcount == a2count)
  {
    return(NULL)
  }
  flag_name = make.unique_(scidb_attributes(join), "source_array_id")
  jf = scidbeval(project(bind(join, name = flag_name, "0"), flag_name))
  lf = project(bind(array1, flag_name, "1"), flag_name)
  rf = project(bind(array2, flag_name, "2"), flag_name)
  merger = scidb(sprintf("merge(%s, %s, %s)", jf@name, lf@name, rf@name))
  subset(merger, sprintf("%s <> 0", flag_name))
}


#' @aliases quantile
#' @importFrom stats quantile
#' @export
quantile.scidb = function(x, probs=seq(0,1,0.25), type=7, ...)
{
  np      = length(probs)
  probs   = pmax(0, pmin(1,probs))  # Filter bogus probabilities out
  if(length(probs)!=np) warning("Probabilities outside [0,1] have been removed.")
  if(length(dim(x))>1) x = scidbeval(project(unpack(x), scidb_attributes(x)[1]), eval=TRUE)
  n = count(x) # * bounds are just wonderful
  x       = sort(x) # Full sort is wasteful! Only really need a partial sort.
  np      = length(probs)
  qs      = NULL

  if(length(scidb_attributes(x))>1)
  {
    warning("The SciDB array contains more than one attribute. Using the first one: ",scidb_attributes(x)[1])
    x = project(x, scidb_attributes(x)[1])
  }
# Check numeric type and quantile type
  ty    = scidb_types(x)[1]
  num   = grepl("int",ty) || grep("float",ty) || grep("double",ty)
  if(!num && type!=1)
  {
    type = 1
    warning("Setting quantile type to 1 to handle non-numeric values")
  }
  start_index = as.numeric(scidb_coordinate_bounds(x)$start)

  if(type==1)
  {
    m       = 0
    j       = floor(n*probs + m)
    g       = n*probs + m - j
    gamma   = as.numeric(g!=0)
    idx     = (1-gamma)*pmax(j,1) + gamma*pmin(j+1,n)
    idx     = idx + start_index - 1
    qs      = subset(x, paste(paste("n=", idx, sep=""), collapse=" or "))
  }
  if(type==7)
  {
    index = start_index + max((n - 1),0) * probs
    lo    = floor(index)
    hi    = ceiling(index)
    i     = index > lo
    gamma = (index - lo)*i + lo*(!i)
    xlo   = subset(x, paste(paste("n=", lo, sep=""), collapse=" or "))[][,2]
    if(length(xlo) < 1) stop("no data")
    xhi   = subset(x, paste(paste("n=", hi, sep=""), collapse=" or "))[][,2]
    qs    = as.scidb((1 - gamma)*xlo + gamma*xhi)
  }
  p = as.scidb(data.frame(probs=probs), start=as.numeric(scidb_coordinate_start(qs)[[1]]))
  merge(p, qs, by.x=dimensions(p), by.y=dimensions(qs))
}


#' Wrapper to SciDB "build" operator
#'
#' The \code{build} function is a wrapper to the SciDB `build` operator.
#' Operation is similar to the R \code{matrix} and \code{array} functions.
#' It creates a new single-attribute SciDB array based on the specified parameters.
#' @param data any valid SciDB expression (expressed as a character string) or constant to fill the array
#' @param dim vector of dimension lengths
#' @param names optional vector of attribute and dimension names; default attribute name is \code{val}
#'        and the dimension names are labeled \code{i, j, ...}
#' @param type SciDB type of the array attribute
#' @param start optional vector of starting dimension coordinate indices. Must match the length of the dim vector
#' @param name optional name of the SciDB array. An automatically generated name is used by default
#' @param chunksize optional vector of dimension chunk sizes. Must match the length of the dim vector
#' @param overlap optional vector of dimension overlap values. Must match the length of the dim vector
#' @param gc \code{TRUE} (the default) removes the array when corresponding R objects are garbage collected
#' @return a \code{scidb} array object
#' @export
#' @examples
#' \dontrun{
#' y <- build(pi, c(5, 3))
#' print(head(y))
#' }
build = function(data, dim, names, type,
                 start, name, chunksize, overlap, gc=TRUE)
{
  if(missing(type))
  {
    type = typeof(data)
    if(is.character(data))
    {
      if(length(grep("\\(",data))>0) type="double"
      else
      {
        type = "string"
        data = sprintf("'%s'",data)
      }
    }
  }
# Special case:
  if(is.scidb(dim))
  {
    schema = sprintf("%s%s",build_attr_schema(dim,I=1),build_dim_schema(dim))
    query = sprintf("build(%s,%s)",schema,data)
    ans = .scidbeval(query)
# We know that the output of build is not sparse
    attr(ans,"sparse") = FALSE
    return(ans)
  }
  if(missing(start)) start = rep(0,length(dim))
  if(missing(overlap)) overlap = rep(0,length(dim))
  if(missing(chunksize))
  {
    chunksize = rep(ceiling(1e6^(1/length(dim))),length(dim))
  }
  if(length(start)!=length(dim)) stop("Mismatched dimension/start lengths")
  if(length(chunksize)!=length(dim)) stop("Mismatched dimension/chunksize lengths")
  if(length(overlap)!=length(dim)) stop("Mismatched dimension/overlap lengths")
  if(missing(names))
  {
    names = c("val", letters[9:(8+length(dim))])
  }
# No scientific notation please
  chunksize = noE(chunksize)
  overlap = noE(overlap)
  dim = noE(dim + (start - 1))
  start = noE(start)
  schema = paste("<",names[1],":",type,">",sep="")
  schema = paste(schema, paste("[",paste(paste(paste(
        paste(names[-1],start,sep="="), dim, sep=":"),
        chunksize, overlap, sep=","), collapse=","),"]",sep=""), sep="")
  query = sprintf("build(%s,%s)",schema,data)
  if(missing(name)) return(.scidbeval(query))
  ans = .scidbeval(query, eval=FALSE, name)
# We know that the output of build is not sparse
  attr(ans,"sparse") = FALSE
  ans
}


t_scidb = function(x)
{
  scidb(sprintf("transpose(%s)", x@name))
}

cov_scidb = function(x, y=NULL, use="everything", method=c("pearson", "kendall", "spearman"))
{
  if(length(dimensions(x)) != 2) stop("x must be a matrix (a 2-d SciDB array)")
  if(length(scidb_attributes(x)) > 1) stop("x must have one numeric attribute")
  if(!is.null(y)) stop("y must be NULL")
  if(use != "everything") stop("only use=everything is supported")
  if(method[1] != "pearson") stop("only Pearson correlation is supported")
  S0 = scidbeval(project(transform(merge(x, aggregate(x, by=dimensions(x)[2], FUN=mean), equi_join=FALSE), x ="val - val_avg"), "x"))
  project(transform(gemm(t(S0), S0), val=sprintf("gemm / %s", as.numeric(scidb_coordinate_bounds(S0)$length[1]) - 1)), "val")
}

cor_scidb = function(x, y=NULL, use="everything", method=c("pearson", "kendall", "spearman"))
{
  if(length(dimensions(x)) != 2) stop("x must be a matrix (a 2-d SciDB array)")
  if(length(scidb_attributes(x)) > 1) stop("x must have one numeric attribute")
  if(!is.null(y)) stop("y must be NULL")
  if(use != "everything") stop("only use=everything is supported")
  if(method[1] != "pearson") stop("only Pearson correlation is supported")
  x = attribute_rename(x, new="val")
  S0 = scidbeval(project(transform(merge(x, aggregate(x, by=dimensions(x)[2], FUN=mean), equi_join=FALSE), x ="val - val_avg"), "x"))
  CV = scidbeval(project(transform(gemm(t(S0), S0), val=sprintf("gemm / %s",
                 as.numeric(scidb_coordinate_bounds(S0)$length[1]) - 1)), "val"), temp=TRUE)
  v = redimension(subset(CV, "x = y"), dim="x")
  v = transform(v, val="1 / sqrt(val)")
  project(transform(merge(merge(CV, v, by.x="x", by.y="x", equi_join=FALSE), v, by.x="y", by.y="x", equi_join=FALSE), cor="val * val_1 * val_2"), "cor")
}
