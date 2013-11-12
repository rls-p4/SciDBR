#/*
#**
#* BEGIN_COPYRIGHT
#*
#* This file is part of SciDB.
#* Copyright (C) 2008-2013 SciDB, Inc.
#*
#* SciDB is free software: you can redistribute it and/or modify
#* it under the terms of the AFFERO GNU General Public License as published by
#* the Free Software Foundation.
#*
#* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
#* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
#* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
#* the AFFERO GNU General Public License for the complete license terms.
#*
#* You should have received a copy of the AFFERO GNU General Public License
#* along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
#*
#* END_COPYRIGHT
#*/

# Create a new scidb reference to an existing SciDB array.
# name (character): Name of the backing SciDB array
#    alternatively, a scidb expression object
# attribute (character): Attribute in the backing SciDB array (applies to n-d arrays)
# gc (logical): Remove backing SciDB array when R object is garbage collected?
scidb = function(name, attribute, gc)
{
  if(missing(name)) stop("array name or expression must be specified")
  if(missing(gc)) gc=FALSE
  query = sprintf("show('%s as hullabaloo','afl')",name)
  schema = iquery(query,re=1)$schema
  obj = scidb_from_schemastring(schema, name)
  if(!missing(attribute))
  {
    if(!(attribute %in% obj@attributes)) warning("Requested attribute not found")
    obj@attribute = attribute
  }
  if(gc)
  {
    obj@gc$name = name
    obj@gc$remove = TRUE
    reg.finalizer(obj@gc, function(e) if (e$remove) 
        tryCatch(scidbremove(e$name), error = function(e) invisible()), 
            onexit = TRUE)
  } else obj@gc = new.env()
  obj
}


colnames.scidb = function(x)
{
  NULL
}

rownames.scidb = function(x)
{
  NULL
}

names.scidb = function(x)
{
  if(is.null(dim(x))) rownames(x)
  colnames(x)
}

`names<-.scidb` = function(x, value)
{
  old = x@attributes
  if(length(value)!=length(old)) stop(paste("Incorrect number of names (should be",length(old),")"))
  arg = paste(paste(old,value,sep=","),collapse=",")
  query = sprintf("attribute_rename(%s,%s)",x@name,arg)
  iquery(query)
}

dimnames.scidb = function(x)
{
  lapply(1:length(x@D$name), function(j)
  {
    if(x@D$type[j] != "string") return(c(x@D$start[j],x@D$start[j]+x@D$length[j]-1))
    if(x@D$length[j] > options("scidb.max.array.elements"))
      stop("Result will be too big. Perhaps try a manual query with an iterative result.")
    Q = sprintf("scan(%s:%s)",x@name,x@D$name[j])
    iquery(Q,return=TRUE,n=Inf)[,2]
  })
}

`dimnames<-.scidb` = function(x, value)
{
  stop("unsupported")
}

summary.scidb = function(x)
{
  warning("Not available.")
  invisible()
}

# XXX this will use insert, write me.
#`[<-.scidb` = function(x,j,k, ..., value)
#{
#  stop("Sorry, scidb array objects are read only for now.")
#}

# Array subsetting wrapper.
# x: A Scidb array object
# ...: list of dimensions
# 
# Returns a materialized R array if length(list(...))==0.
# Or, a scidb subrray promise.
`[.scidb` = function(x, ...)
{
  M = match.call()
  drop = ifelse(is.null(M$drop),TRUE,M$drop)
  eval = ifelse(is.null(M$eval),FALSE,M$eval)
  M = M[3:length(M)]
  if(!is.null(names(M))) M = M[!(names(M) %in% c("drop","eval"))]
# i shall contain a list of requested index values
  E = parent.frame()
  i = lapply(1:length(M), function(j) tryCatch(eval(M[j][[1]],E),error=function(e)c()))
# User wants this materialized to R...
  if(all(sapply(i,is.null)))
    return(materialize(x,drop=drop))
# Not materializing, return a SciDB array
  if(length(i)!=length(dim(x))) stop("Dimension mismatch")
  dimfilter(x,i,eval)
}

`dim.scidb` = function(x)
{
  if(length(x@dim)==0) return(NULL)
  x@dim
}

`dim<-.scidb` = function(x, value)
{
  stop("unsupported")
}


`str.scidb` = function(object, ...)
{
  cat("SciDB array name: ",object@name)
  cat("\tattribute in use: ",object@attribute)
  cat("\nAll attributes: ",object@attributes)
  cat("\nArray dimensions:\n")
  cat(paste(capture.output(print(data.frame(object@D))),collapse="\n"))
  cat("\n")
}

`print.scidb` = function(x, ...)
{
  cat("\nSciDB array ",x@name)
  if(nchar(x@attribute)>0)
    cat("\tattribute: ",x@attribute)
  cat("\n")
  print(head(x))
  if(is.null(x@dim)) j = x@length - 6
  else j = x@dim[1]-6
  if(j>2) cat("and ",j,"more rows not displayed...\n")
  if(length(x@dim)>0) {
    k = x@dim[2] - 6
    if(k>2) cat("and ",k,"more columns not displayed...\n")
  }
}

`ncol.scidb` = function(x) x@dim[2]
`nrow.scidb` = function(x) x@dim[1]
`dim.scidb` = function(x) {if(length(x@dim)>0) return(x@dim); NULL}
`length.scidb` = function(x) x@length

# Vector, Matrix, matrix, or data.frame only.
as.scidb = function(X,
                    name=tmpnam(),
                    rowChunkSize,
                    colChunkSize,
                    start=c(0L,0L),
                    gc=TRUE, ...)
{
  if(inherits(X,"data.frame"))
    if(missing(rowChunkSize))
      return(df2scidb(X,name=name,gc=gc,...))
    else
      return(df2scidb(X,name=name,chunkSize=rowChunkSize,gc=gc,...))
  if(inherits(X,"dgCMatrix"))
  {
    rowChunkSize = min(1000L,nrow(X))
    colChunkSize = min(1000L,ncol(X))
    return(.Matrix2scidb(X,name=name,rowChunkSize=rowChunkSize,colChunkSize=colChunkSize,start=start,gc=gc,...))
  }
  D = dim(X)
  rowOverlap=0L
  colOverlap=0L
  if(length(start)<1) stop ("Invalid starting coordinates")
  if(length(start)>2) start = start[1:2]
  if(length(start)<2) start = c(start, 0)
  start = as.integer(start)
  type = .scidbtypes[[typeof(X)]]
  if(is.null(type)) {
    stop(paste("Unupported data type. The package presently supports: ",
       paste(.scidbtypes,collapse=" "),".",sep=""))
   }
  if(is.null(D)) {
# X is a vector
    if(!is.vector(X)) stop ("X must be a matrix or a vector")
    if(missing(rowChunkSize)) rowChunkSize = min(1e6, length(X))
    X = as.matrix(X)
    schema = sprintf(
      "< val : %s >  [i=%.0f:%.0f,%.0f,%.0f]", type, start[[1]],
      nrow(X)-1+start[[1]], min(nrow(X),rowChunkSize), rowOverlap)
  } else {
# X is a matrix
    if(missing(rowChunkSize)) rowChunkSize = min(1000L, nrow(X))
    if(missing(colChunkSize)) colChunkSize = min(1000L, ncol(X))
    schema = sprintf(
      "< val : %s >  [i=%.0f:%.0f,%.0f,%.0f, j=%.0f:%.0f,%.0f,%.0f]", type, start[[1]],
      nrow(X)-1+start[[1]], min(nrow(X),rowChunkSize), rowOverlap, start[[2]], ncol(X)-1+start[[2]],
      min(ncol(X),colChunkSize), colOverlap)
  }
  if(!is.matrix(X)) stop ("X must be a matrix or a vector")

# Obtain a session from shim for the upload process
  session = getSession()
  on.exit( GET("/release_session",list(id=session)) ,add=TRUE)

# Upload the data
# XXX I couldn't get RCurl to work using the fileUpload(contents=x), with 'x'
# a raw vector. But we need RCurl to support SSL. As a work-around, we save
# the object. This copy sucks and must be fixed.
  fn = tempfile()
  bytes = writeBin(as.vector(t(X)),con=fn)
  url = URI("upload_file",list(id=session))
  ans = postForm(uri = url, uploadedfile = fileUpload(filename=fn),
           .opts = curlOptions(httpheader = c(Expect = ""),'ssl.verifypeer'=0))
  unlink(fn)
  ans = ans[[1]]
  ans = gsub("\r","",ans)
  ans = gsub("\n","",ans)

# Load query
  query = sprintf("store(input(%s,'%s', 0, '(%s)'),%s)",schema,ans,type,name)
  iquery(query)
  ans = scidb(name,gc=gc)
  ans
}


# Transpose array
t.scidb = function(x,eval=FALSE)
{
  query = sprintf("transpose(%s)",x@name)
  scidbeval(query,eval=eval,gc=TRUE)
}
