#' Upload R data to SciDB
#' Data are uploaded as TSV.
#' @param X an R data frame, raw value, Matrix, matrix, or vector object
#' @param name a SciDB array name to use
#' @param start starting SciDB integer coordinate index
#' @param gc set to FALSE to disconnect the SciDB array from R's garbage collector
#' @param ... other options, see \code{\link{df2scidb}}
#' @return A \code{scidb} object
#' @export
as.scidb = function(X,
                    name=tmpnam(),
                    start,
                    gc=TRUE, ...)
{
  if(inherits(X, "raw"))
  {
    return(raw2scidb(X, name=name, gc=gc,...))
  }
  if(inherits(X, "data.frame"))
  {
    return(df2scidb(X, name=name, gc=gc, start=start, ...))
  }
  if(inherits(X, "dgCMatrix"))
  {
    return(.Matrix2scidb(X, name=name, start=start, gc=gc, ...))
  }
  return(matvec2scidb(X, name=name, start=start, gc=gc, ...))
}

#' Internal function to upload an R data frame to SciDB
#' @param X a data frame
#' @param name SciDB array name
#' @param dimlabel name of SciDB dimension
#' @param chunkSize SciDB chunk size
#' @param rowOverlap SciDB chunk overlap
#' @param types SciDB attribute types
#' @param nullable SciDB attribute nullability
#' @param schema_only set to \code{TRUE} to just return the SciDB schema (don't upload)
#' @param gc set to \code{TRUE} to connect SciDB array to R's garbage collector
#' @param start SciDB coordinate index starting value
#' @return A \code{\link{scidb}} object, or a character schema string if \code{schema_only=TRUE}.
#' @keywords internal
df2scidb = function(X,
                    name=tmpnam(),
                    dimlabel="row",
                    chunkSize,
                    rowOverlap=0L,
                    types=NULL,
                    nullable,
                    schema_only=FALSE,
                    gc, start)
{
  if(!is.data.frame(X)) stop("X must be a data frame")
  if(missing(start)) start = 1
  start = as.numeric(start)
  if(missing(gc)) gc = FALSE
  if(!missing(nullable)) warning("The nullable option has been deprecated. All uploaded attributes will be nullable by default. Use the `replaceNA` function to change this.")
  nullable = TRUE
  anames = make.names(names(X), unique=TRUE)
  anames = gsub("\\.","_", anames, perl=TRUE)
  if(length(anames)!=ncol(X)) anames = make.names(1:ncol(X))
  if(!all(anames==names(X))) warning("Attribute names have been changed")
# Check for attribute/dimension name conflict
  old_dimlabel = dimlabel
  dimlabel = tail(make.names(c(anames, dimlabel),unique=TRUE), n=1)
  dimlabel = gsub("\\.","_", dimlabel, perl=TRUE)
  if(dimlabel!=old_dimlabel) warning("Dimension name has been changed")
  if(missing(chunkSize)) {
    chunkSize = min(nrow(X), 10000)
  }
  chunkSize = as.numeric(chunkSize)
  m = ceiling(nrow(X) / chunkSize)

# Default type is string
  typ = rep("string", ncol(X))
  dcast = anames
  if(!is.null(types)) {
    for(j in 1:ncol(X)) typ[j] = types[j]
  } else {
    for(j in 1:ncol(X)) {
      if("numeric" %in% class(X[,j])) 
        typ[j] = "double"
      else if("integer" %in% class(X[,j])) 
        typ[j] = "int32"
      else if("logical" %in% class(X[,j])) 
        typ[j] = "bool"
      else if("character" %in% class(X[,j])) 
        typ[j] = "string"
      else if("factor" %in% class(X[,j])) 
      {
        if("scidb_factor" %in% class(X[,j]))
        {
          typ[j] = paste("int64")
          Xj = X[,j]
          levels(Xj) = levels_scidb(Xj)
          X[,j] = as.vector(Xj)
        }
        else
          typ[j] = "string"
      }
      else if("POSIXct" %in% class(X[,j])) 
      {
        warning("Converting R POSIXct to SciDB datetime as UTC time. Subsecond times rounded to seconds.")
        X[,j] = format(X[,j],tz="UTC")
        typ[j] = "datetime"
      }
    }  
  }
  for(j in 1:ncol(X))
  {
    if(typ[j] == "datetime") dcast[j] = sprintf("%s, datetime(a%d)",anames[j],j-1)
    else if(typ[j] == "string") dcast[j] = sprintf("%s, a%d", anames[j], j-1)
    else dcast[j] = sprintf("%s, dcast(a%d, %s(null))", anames[j], j-1, typ[j])
  }
  args = sprintf("<%s>", paste(anames, ":", typ, " null", collapse=","))

  SCHEMA = paste(args,"[",dimlabel,"=",noE(start),":",noE(nrow(X)+start-1),",",noE(chunkSize),",", noE(rowOverlap),"]",sep="")
  if(schema_only) return(SCHEMA)

# Obtain a session from the SciDB http service for the upload process
  session = getSession()
  on.exit(SGET("/release_session", list(id=session), err=FALSE) ,add=TRUE)

  ncolX = ncol(X)
  X = charToRaw(paste(capture.output(write.table(X, sep="\t", row.names=FALSE, col.names=FALSE, quote=FALSE)), collapse="\n"))
  tmp = POST(X, list(id=session))
  tmp = gsub("\n", "", gsub("\r", "", tmp))

# Generate a load_tools query
  LOAD = sprintf("apply(parse(split('%s'),'num_attributes=%d'),%s)", tmp,
                 ncolX, paste(dcast, collapse=","))
  query = sprintf("store(redimension(%s,%s),%s)", LOAD, SCHEMA, name)
  scidbquery(query, release=1, session=session, stream=0L)
  scidb(name, gc=gc)
}


cache = function(x, name)
{
  if(!is.raw(x)) stop("x must be a raw value")
  if(missing(name)) return(CACHE(x, list(sync=0)))
  CACHE(x, list(sync=0, name=name))
}

uncache = function(name, remove=FALSE)
{
  remove = as.integer(remove)
  SGET("/uncache", list(name=name, remove=remove), binary=TRUE)
}



.Matrix2scidb = function(X, name, rowChunkSize=1000, colChunkSize=1000, start=c(0,0), gc=TRUE, ...)
{
  D = dim(X)
  N = Matrix::nnzero(X)
  rowOverlap=0L
  colOverlap=0L
  if(length(start)<1) stop ("Invalid starting coordinates")
  if(length(start)>2) start = start[1:2]
  if(length(start)<2) start = c(start, 0)
  start = as.integer(start)
  type = .Rtypes[[typeof(X@x)]]
  if(is.null(type)) {
    stop(paste("Unupported data type. The package presently supports: ",
       paste(unique(names(.Rtypes)), collapse=" "), ".", sep=""))
  }
  if(type!="double") stop("Sorry, the package only supports double-precision sparse matrices right now.")
  schema = sprintf(
      "< val : %s null>  [i=%.0f:%.0f,%.0f,%.0f, j=%.0f:%.0f,%.0f,%.0f]", type, start[[1]],
      nrow(X)-1+start[[1]], min(nrow(X),rowChunkSize), rowOverlap, start[[2]], ncol(X)-1+start[[2]],
      min(ncol(X),colChunkSize), colOverlap)
  schema1d = sprintf("<i:int64 null, j:int64 null, val : %s null>[idx=0:*,100000,0]",type)

# Obtain a session from shim for the upload process
  session = getSession()
  if(length(session)<1) stop("SciDB http session error")
  on.exit(SGET("/release_session",list(id=session), err=FALSE) ,add=TRUE)

# Compute the indices and assemble message to SciDB in the form
# double,double,double for indices i,j and data val.
  dp = diff(X@p)
  j  = rep(seq_along(dp),dp) - 1

# Upload the data
  bytes = .Call("scidb_raw",as.vector(t(matrix(c(X@i + start[[1]],j + start[[2]], X@x),length(X@x)))),PACKAGE="scidb")
  ans = POST(bytes, list(id=session))
  ans = gsub("\n", "", gsub("\r", "", ans))

# redimension into a matrix
  query = sprintf("store(redimension(input(%s,'%s',-2,'(double null,double null,double null)'),%s),%s)",schema1d, ans, schema, name)
  iquery(query)
  scidb(name,gc=gc)
}


matvec2scidb = function(X,
                        name=tmpnam(),
                        start,
                        gc=TRUE, ...)
{
# Check for a bunch of optional hidden arguments
  args = list(...)
  attr_name = "val"
  nullable = TRUE
  if(!is.null(args$nullable)) nullable = as.logical(args$nullable) # control nullability
  if(!is.null(args$attr)) attr_name = as.character(args$attr)      # attribute name
  do_reshape = TRUE
  nd_reshape = NULL
  type = force_type = .Rtypes[[typeof(X)]]
  if(is.null(type)) {
    stop(paste("Unupported data type. The package presently supports: ",
       paste(unique(names(.Rtypes)), collapse=" "), ".", sep=""))
  }
  if(!is.null(args$reshape)) do_reshape = as.logical(args$reshape) # control reshape
  if(!is.null(args$type)) force_type = as.character(args$type) # limited type conversion
  chunkSize = c(min(1000L, nrow(X)), min(1000L, ncol(X)))
  chunkSize = as.numeric(chunkSize)
  if(length(chunkSize) == 1) chunkSize = c(chunkSize, chunkSize)
  overlap = c(0,0)
  if(missing(start)) start = c(0,0)
  start     = as.numeric(start)
  if(length(start) ==1) start = c(start, start)
  D = dim(X)
  start = as.integer(start)
  overlap = as.integer(overlap)
  dimname = make.unique_(attr_name, "i")
  if(is.null(D))
  {
# X is a vector
    if(!is.vector(X)) stop ("Unsupported object")
    do_reshape = FALSE
    chunkSize = min(chunkSize[[1]], length(X))
    X = as.matrix(X)
    schema = sprintf(
        "< %s : %s null>  [%s=%.0f:%.0f,%.0f,%.0f]", attr_name, force_type, dimname, start[[1]],
        nrow(X) - 1 + start[[1]], min(nrow(X), chunkSize), overlap[[1]])
    load_schema = schema
  } else if(length(D) > 2)
  {
    nd_reshape = dim(X)
    do_reshape = FALSE
    X = as.matrix(as.vector(aperm(X)))
    schema = sprintf(
        "< %s : %s null>  [%s=%.0f:%.0f,%.0f,%.0f]", attr_name, force_type, dimname, start[[1]],
        nrow(X) - 1 + start[[1]], min(nrow(X), chunkSize), overlap[[1]])
    load_schema = sprintf("<%s:%s null>[__row=1:%.0f,1000000,0]", attr_name, force_type, length(X))
  } else {
# X is a matrix
    schema = sprintf(
      "< %s : %s  null>  [i=%.0f:%.0f,%.0f,%.0f, j=%.0f:%.0f,%.0f,%.0f]", attr_name, force_type, start[[1]],
      nrow(X) - 1 + start[[1]], chunkSize[[1]], overlap[[1]], start[[2]], ncol(X) - 1 + start[[2]],
      chunkSize[[2]], overlap[[2]])
    load_schema = sprintf("<%s:%s null>[__row=1:%.0f,1000000,0]", attr_name, force_type,  length(X))
  }
  if(!is.matrix(X)) stop ("X must be a matrix or a vector")

  DEBUG = FALSE
  if(!is.null(options("scidb.debug")[[1]]) && TRUE == options("scidb.debug")[[1]]) DEBUG=TRUE
  td1 = proc.time()
# Obtain a session from shim for the upload process
  session = getSession()
  on.exit( SGET("/release_session", list(id=session), err=FALSE) ,add=TRUE)

# Upload the data
  bytes = .Call("scidb_raw", as.vector(t(X)), PACKAGE="scidb")
  ans = POST(bytes, list(id=session))
  ans = gsub("\n", "", gsub("\r", "", ans))
  if(DEBUG)
  {
    cat("Data upload time", (proc.time() - td1)[3], "\n")
  }

# Load query
  if(!is.null(nd_reshape))
  {
    return(scidbeval(reshape_scidb(scidb(sprintf("input(%s,'%s', 0, '(%s null)')",load_schema, ans, type)), shape=nd_reshape),name=name))
  }
  if(do_reshape)
  {
    query = sprintf("store(reshape(input(%s,'%s', -2, '(%s null)'),%s),%s)",load_schema, ans, type, schema, name)
  } else
  {
    query = sprintf("store(input(%s,'%s', -2, '(%s null)'),%s)",load_schema, ans, type, name)
  }
  iquery(query)
  ans = scidb(name, gc=gc)
  if(!nullable) ans = replaceNA(ans)
  ans
}
