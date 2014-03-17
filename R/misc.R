na.locf_scidb = function(object, along=object@D$name[1],`eval`=FALSE)
{
  i = which(object@D$name == along)
  if(length(along)!=1 || length(i)!=1) stop("Please specify exactly one dimension to run along.")
# Make object nullable
  object = make_nullable(object)
# Set up a bounding box that contains the data.
  aname = make.unique_(c(object@attributes, object@D$name),object@D$name)
  expr = paste(paste("min(",aname,"), max(", aname,")",sep=""),collapse=",")
  limits = matrix(unlist(aggregate(bind(object, aname, object@D$name), FUN=expr, unpack=FALSE)[]),nrow=2)
# limits is a 2 x length(dim(object)) matrix. The first row contains the min
# dim values, and the 2nd row the max dim values.
  reschema = sprintf("%s%s",build_attr_schema(object),
               build_dim_schema(object,newend=limits[2,],newstart=limits[1,]))
  object = redimension(object, reschema)

# Build a null-merge array
  N = sprintf("build(%s%s,null)",build_attr_schema(object,I=1), build_dim_schema(object))
  if(length(object@attributes)>1)
  {
    vals = paste(object@types[-1],"(null)",sep="")
    N = sprintf("apply(%s, %s)", N, paste(paste(object@attributes[-1],vals,sep=","),collapse=","))
  }
  query = sprintf("merge(%s,%s)",object@name, N)

# Run the na.locf
  impute = paste(paste("last_value(",object@attributes,") as ", object@attributes ,sep=""),collapse=",")
  query = sprintf("cumulate(%s, %s, %s)", query, impute, along)
  .scidbeval(query,depend=list(object),`eval`=eval,gc=TRUE)
}


hist_scidb = function(x, breaks=10, right=FALSE, materialize=TRUE, `eval`=FALSE, `plot`=TRUE, ...)
{
  if(length(x@attributes)>1) stop("Histogram requires a single-attribute array.")
  if(length(breaks)>1) stop("The SciDB histogram function requires a single numeric value indicating the number of breaks.")
  a = x@attributes[1]
  t = x@types[1]
  breaks = as.integer(breaks)
  if(breaks < 1) stop("Too few breaks")
# name of binning coordinates in output array:
  d = make.unique_(c(a,x@D$name), "bin")
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
