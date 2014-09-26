# This is a catch-all container file for useful miscellaneous functions. They
# tend to be newer and somewhat more experimental than the other functions, and
# maybe not quite as fully baked.

bernoulli = function (x, prob , seed=sample(2^32 - 1 - 2^31, 1))
{
  if ( prob <= 0 || prob > 1 )
  {
    stop("Invalid prob value")
  }
  query = sprintf("bernoulli(%s, %.16f, %d)", x@name, prob, seed)
  return (scidb(query))
}

peek = function(x, n=6L, prob=0.1,...)
{
  subarray(unpack(bernoulli(x,prob,...)),c(0,n-1))[]
}

order_scidb = function(x,na.last=TRUE,decreasing=FALSE)
{
# XXX Does this dispatch properly to other methods for order?
  if(!is.scidb(x)) return(base::order(x, na.last=na.last, decreasing=decreasing))
  if(!na.last) stop("The na.last argument is not supported")
  if(length(dim(x))>1) stop("x must be a scidb vector object")
  a = bind(x,"p",dimensions(x)[1])
  s = sort(a,attributes=scidb_attributes(a)[-length(scidb_attributes(a))],decreasing=decreasing)
# XXX modify this to return the same dimension schema as x
  project(s,length(scidb_attributes(s)))[0:(count(s)-1)]
}


rank_scidb = function(x,na.last=TRUE,ties.method = c("average", "first", "random", "max", "min"))
{
  if(!is.scidb(x)) stop("x must be a scidb vector object")
  if(length(dim(x))>1) stop("x must be a scidb vector object")
  attribute=scidb_attributes(x)[1]
  dimension=""
  ties.method = match.arg(ties.method)
  op = ifelse(ties.method=="average","avg_rank","rank")
  query = sprintf("%s(%s,%s%s)",op,x@name,attribute,dimension)
  .scidbeval(query,depend=list(x),eval=TRUE)
}

kmeans_scidb = function(x, centers, iter.max=30, nstart=1,
  algorithm="Lloyd")
{
  if(length(dim(x))!=2) stop("x must be a matrix")
  if(nstart!=1 || algorithm!="Lloyd") stop("This version limited to Lloyd's method with nstart=1")
# If we have a recent enough SciDB version, use temp arrays.
  temp = compare_versions(options("scidb.version")[[1]],14.8)
  if(!is.scidb(x)) stop("x must be a scidb object")
  x = project(x, x@attributes[1])
  x = attribute_rename(x,new="val")
  x = dimension_rename(x,new=c("i","j"))
  expr = sprintf("random() %% %d", centers)
  group = scidbeval(build(expr, nrow(x), names=c("group","i"), type="int64"), temp=temp)
  dist_name = NULL
  k = centers
  diff_name = NULL
  for(iter in 1:iter.max)
  {
    centers = aggregate(x, by=list(group, "j"), FUN=mean, eval=TRUE)
    dist = scidbeval(
             aggregate(
               bind(
                 merge(x, centers, by="j"),
                   "dist", "(val - val_avg)*(val - val_avg)"
               ),
               by=list("i","group"),
               FUN="sum(dist) as dist", unpack=FALSE)
            ,temp=temp, name=dist_name)
    dist_name = dist@name
    oldgroup = group
    group = scidbeval(redimension(
               Filter("dist = min",
                 merge(dist,
                       aggregate(dist,by="i", FUN="min(dist) as min", unpack=FALSE),
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


# Limited distance function (Euclidean only) SLOOOOOW!
dist_scidb = function(x)
{
# Slow...
#  u = apply(x*x,1,sum) %*% matrix(1.0,1,nrow(x))
#  ans = sqrt(abs(u + t(u) - 2 * x %*% t(x)))

# The following is faster but still slow:
  n   = nrow(x)
  x1  = merge(x,x)
  p = make.unique_(x1@attributes,"prod")
  x2  = project(bind(x1,p,paste(x1@attributes,collapse="*")),p)
  s = scidbeval(apply(x2, 1, sum)) # Cache this result
  r = dimensions(s)

  u  = project(merge(build(1,c(n,n),names=c("val",dimensions(x))),s,by=r),2)
  tu = project(merge(build(1,c(n,n),names=c("val",dimensions(x))),s,by.x=dimensions(x)[2],by.y=r),2)

  utu = merge(u,tu)
  utu = project(bind(utu,"sum",paste(scidb_attributes(utu),collapse="+")),"sum")

  out = build(0,c(nrow(x),nrow(x)), type="double")

  xtx = scidb(sprintf("gemm(%s, transpose(%s), %s)",x@name,x@name,out@name))
  project(bind(merge(utu,xtx),"ans","sqrt(abs(sum - 2*gemm))"),"ans")
}

# Note fill_sparse=TRUE is not yet supported but will be...
na.locf_scidb = function(object, along=dimensions(object)[1],fill_sparse=FALSE, `eval`=FALSE)
{
  dnames = dimensions(object)
  i = which(dnames == along)
  if(length(along)!=1 || length(i)!=1) stop("Please specify exactly one dimension to run along.")
# Make object nullable
  object = make_nullable(object)
# Set up a bounding box that contains the data.
  aname = make.unique_(c(object@attributes, dnames),dnames)
  expr = paste(paste("min(",aname,"), max(", aname,")",sep=""),collapse=",")
  limits = matrix(unlist(aggregate(bind(object, aname, dnames), FUN=expr, unpack=FALSE)[]),nrow=2)
# limits is a 2 x length(dim(object)) matrix. The first row contains the min
# dim values, and the 2nd row the max dim values.
  reschema = sprintf("%s%s",build_attr_schema(object),
               build_dim_schema(object,newend=limits[2,],newstart=limits[1,]))
  object = redimension(object, reschema)

# Build a null-merge array
#  object = merge(object, project(merge(build("null",dim=object,type="double"),apply(object,1,min), merge=fill_sparse),1:length(object@attributes)),merge=TRUE)
  object = merge(object,project(merge(bind(attribute_rename(apply(object,1,min),new=paste(object@attributes,"___",sep="")),"price","double(null)"),apply(object,2,min)),object@attributes),merge=TRUE)

# Run the na.locf
  impute = paste(paste("last_value(",object@attributes,") as ", object@attributes ,sep=""),collapse=",")
  query = sprintf("cumulate(%s, %s, %s)", object@name, impute, along)
  .scidbeval(query,depend=list(object),`eval`=eval,gc=TRUE)
}

hist_scidb = function(x, breaks=10, right=FALSE, materialize=TRUE, `eval`=FALSE, `plot`=TRUE, ...)
{
  if(length(x@attributes)>1) stop("Histogram requires a single-attribute array.")
  if(length(breaks)>1) stop("The SciDB histogram function requires a single numeric value indicating the number of breaks.")
  a = x@attributes[1]
  t = scidb_types(x)[1]
  breaks = as.integer(breaks)
  if(breaks < 1) stop("Too few breaks")
# name of binning coordinates in output array:
  d = make.unique_(c(a,dimensions(x)), "bin")
  M = .scidbeval(sprintf("aggregate(%s, min(%s) as min, max(%s) as max)",x@name,a,a),`eval`=TRUE)
  FILL = sprintf("slice(cross_join(build(<counts: uint64 null>[%s=0:%.0f,1000000,0],0),%s),i,0)", d, breaks,M@name)
  if(`right`)
  {
    query = sprintf("project( apply( merge(redimension( substitute( apply(cross_join(%s,%s), %s,iif(%s=min,1,ceil(%.0f.0*(%s-min)/(0.0000001+max-min)))  ),build(<v:int64>[i=0:0,1,0],0),%s), <counts:uint64 null, min:%s null, max:%s null>[%s=0:%.0f,1000000,0], count(%s) as counts),%s), breaks, %s*(0.0000001+max-min)/%.0f.0 + min), breaks,counts)", x@name, M@name, d, a, breaks, a, d, t, t, d, breaks, d, FILL, d, breaks)
  } else
  {
    query = sprintf("project( apply( merge(redimension( substitute( apply(cross_join(%s,%s), %s,floor(%.0f.0 * (%s-min)/(0.0000001+max-min))),build(<v:int64>[i=0:0,1,0],0),%s), <counts:uint64 null, min:%s null, max:%s null>[%s=0:%.0f,1000000,0], count(%s) as counts), %s) , breaks, %s*(0.0000001+max-min)/%.0f.0 + min), breaks,counts)", x@name, M@name, d, breaks, a, d, t, t, d, breaks, d, FILL, d, breaks)
  }
  if(!materialize)
  {
# Return a SciDB array that represents the histogram breaks and counts
    return(.scidbeval(query,depend=list(x,M),`eval`=`eval`,gc=TRUE,`data.frame`=TRUE))
  }
# Return a standard histogram object
  ans = as.list(.scidbeval(query,depend=list(x,M),`eval`=`eval`,gc=TRUE,`data.frame`=TRUE)[])
# Cull the trailing zero bin to correspond to R's output
  if(`right`) ans$counts = ans$counts[-1]
  else ans$counts = ans$counts[-length(ans$counts)]
  ans$density = 0.01*ans$counts/diff(ans$breaks)
  ans$mids = ans$breaks[-length(ans$breaks)] + diff(ans$breaks)/2
  ans$equidist = TRUE
  ans$xname = a
  class(ans) = "histogram"
  MC = match.call()
  if(!`plot`) return (ans)
  plot(ans, ...)
  ans
}


# Several nice functions contributed by Alex Poliakov follow...

# Return TRUE if array1 has the same dimensions, same attributes and types and
# same data at the same coordinates False otherwise
all.equal.scidbdf = function ( target, current , ...)
{
  all.equal.scidb( target, current )
}
all.equal.scidb = function ( target, current , ...)
{
  array1 = target
  array2 = current
  if ( length(array1@attributes) != length(array2@attributes) )
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

# Given two arrays of same dimensionality, return any coordinates that do NOT
# join. For all coordinates, the single attribute shall equal to 1 if those
# coordinates exist in array1, or 2 if those coordinates exist in array2.
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
  flag_name = make.unique_(join@attributes, "source_array_id")
  jf = scidbeval(project(bind(join, name = flag_name, "0"), flag_name))
  lf = project(bind(array1, flag_name, "1"), flag_name)
  rf = project(bind(array2, flag_name, "2"), flag_name)
  merger = scidb(sprintf("merge(%s, %s, %s)", jf@name, lf@name, rf@name))
  subset(merger, sprintf("%s <> 0", flag_name))
}


quantile.scidbdf = function(x, probs=seq(0,1,0.25), type=7, ...)
{
  quantile.scidb(x,probs,type,...)
}
quantile.scidb = function(x, probs=seq(0,1,0.25), type=7, ...)
{
  np      = length(probs)
  probs   = pmax(0, pmin(1,probs))  # Filter bogus probabilities out
  if(length(probs)!=np) warning("Probabilities outside [0,1] have been removed.")
  if(length(dim(x))>1) x = project(unpack(x),scidb_attributes(x)[1],eval=TRUE)
  n = count(x) # * bounds are just wonderful
  x       = sort(x) # Full sort is wasteful! Only really need a partial sort.
  np      = length(probs)
  qs      = NULL

  if(length(x@attributes)>1)
  {
    warning("The SciDB array contains more than one attribute. Using the first one: ",x@attributes[1])
    x = project(x,x@attributes[1])
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
    qs      = x[idx]
  }
  if(type==7)
  {
    index = start_index + max((n - 1),0) * probs
    lo    = floor(index)
    hi    = ceiling(index)
    i     = index > lo
    gamma = (index - lo)*i + lo*(!i)
    xlo   = as.numeric(x[lo][]) # Needed to cast potential sparse vectors
    xhi   = as.numeric(x[hi][])
    qs    = as.scidb((1 - gamma)*xlo + gamma*xhi)
  }
  p = as.scidb(data.frame(probs=probs),start=as.numeric(scidb_coordinate_start(qs)[[1]]))
  merge(p,qs,by.x=dimensions(p),by.y=dimensions(qs))
}
