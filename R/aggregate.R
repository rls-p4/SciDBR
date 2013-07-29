# Right now aggregate looks like:

# aggregate(x, . ~ Species, "min(PetalWidth) as p, min(PetalLength) as z")

# instead it shold look like:

#aggregate(x, PetalWidth + PetalLength ~ Species, "min")





# Reasonably general SciDB aggregation interface The input must either use a
# formula interface or the by interface.  R's built in aggregate function
# dispatches using S3, which means it can only dispatch on its first argument.
# To make things easy, all SciDB uses of aggregate begin with a SciDB array
# argument.  The formula interface looks like:
#
# aggregate(X, formula=Petal_Width ~ Species, FUN="avg(Petal_Width) as mean")
# where,
# X is a 1D SciDB array containing attributes Petal_Width and Species. FUN
# is a character string representing any valid SciDB aggregation expression
# with named output.
#
# The by interface looks like:
#
# aggregate(X, by=list(...), FUN="scidb aggregation expression",cross_join)
# where,
# X is any SciDB array,
# by is a list of any of the following:
# character string named dimensions of X to aggregate along, and/or,
# a single scidb array that contains one or more dimensions that can be
# joined with X
# and one or more int64 attributes to aggregate on, and/or,
# a character string naming attributes of X to aggregate along.
# cross_join is an optional list of  dimension namess to join on. If
# missing, attempt to join on common dimension names.
# If only one thing is specified, a scalar works instead of a list.
#
# Example 1:
# aggregate(iris, by="Species", FUN="avg(Petal_length) as mean")

# Experimental

aggregate.scidb = function(x,formula,FUN,by,cross_join)
  {
    data=x
    if(missing(formula)) stop("Usage: aggregate(1D_scidb_array, formula, scidb_aggregate_expression)")
    if(missing(FUN)) stop("Usage: aggregate(1D_scidb_array, formula, scidb_aggregate_expression)")
# This is a hack until list('aggregates') returns type information, right now
# we have to guess!!!
    agtypes = list(approxdc="uint64 NULL",avg="double NULL",count="uint64 NULL",stdev="double NULL",var="double NULL")
    if(missing(FUN)) stop("You must supply a SciDB aggregate expression")
    v = attr(terms(formula),"term.labels")
    r = setdiff(all.vars(formula),v)
    if(r==".") r = setdiff(data@attributes,v)
# Redimension
    A = tmpnam()

    agat = strsplit(FUN,",")[[1]]
browser()
    agnames = gsub(".* ","", gsub(" *$","",gsub("^ *","",gsub(".*)","",agat))))

    atnames = strsplit(FUN,split="\\(")[[1]]
    wx = grep("\\)",atnames)
    if(length(wx)>0) atnames = gsub("\\).*","",atnames[wx])
# Default aggregate type is same as attribute + NULL
    agtp = unlist(lapply(atnames,function(z)data@types[data@attributes %in% z]))
    agtp = paste(agtp, "NULL")

    if(any(nchar(agnames))<1) stop("We require that aggregate expressions name outputs, for example count(x) AS cx")
# Check for aggregates with special types:
    agfun = tolower(gsub(" *","",gsub("\\(.*","",agat)))
    J = agfun %in% names(agtypes)
    if(any(J)) agtp[J] = agtypes[agfun[J]]
    attributes = paste(paste(agnames,agtp,sep=":"),collapse=",")

  scipen = options("scipen")
  options(scipen=20)
  on.exit(options(scipen))

    chunks = rep(1,length(v))
    chunks[length(chunks)] = data@D$length
    dims = paste(v,data@types[data@attributes %in% v],sep="(")
    dims = paste(dims,"*",sep=")=")
    dims = paste(dims,chunks,sep=",")
    dims = paste(dims,",0",sep="")
    dims = paste(dims,collapse=",")
    schema = sprintf("<%s>[%s]",attributes,dims)
    query = sprintf("create_array(%s,%s)",A, schema)
    iquery(query)
    on.exit(tryCatch(scidbremove(A),error=function(e)invisible()))
    query = sprintf("redimension_store(%s,%s,%s)",data@name, A, paste(FUN,collapse=","))
    iquery(query)
    query = sprintf("scan(%s)",A)
    iquery(query, `return`=TRUE, n=Inf)
  }
