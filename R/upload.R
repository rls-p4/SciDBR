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
#' @return A \code{\link{scidbdf}} object, or a character schema string if \code{schema_only=TRUE}.
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
  on.exit(GET("/release_session", list(id=session), err=FALSE) ,add=TRUE)

  ncolX = ncol(X)
  X = charToRaw(paste(capture.output(write.table(X, sep="\t", row.names=FALSE, col.names=FALSE, quote=FALSE)), collapse="\n"))
  tmp = POST(X, list(id=session))
  tmp = gsub("\n", "", gsub("\r", "", tmp))

# Generate a load_tools query
  LOAD = sprintf("apply(parse(split('%s'),'num_attributes=%d'),%s)", tmp,
                 ncolX, paste(dcast, collapse=","))
  query = sprintf("store(redimension(%s,%s),%s)", LOAD, SCHEMA, name)
  scidbquery(query, async=FALSE, release=1, session=session, stream=0L)
  scidb(name, gc=gc)
}



#' Upload R data to SciDB
#' Data are uploaded as TSV.
#' @param X an R data frame or raw value
#' @param name a SciDB array name to use
#' @param start starting SciDB integer coordinate index
#' @param gc set to FALSE to disconnect the SciDB array from R's garbage collector
#' @param ... other options, see \code{\link{df2scidb}}
#' @return A \code{scidbdf} object
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
    if(missing(chunksize))
      return(df2scidb(X, name=name, gc=gc, start=start, ...))
    else
      return(df2scidb(X, name=name, chunkSize=as.numeric(chunksize[[1]]), gc=gc, start=start,...))
  }
  stop("X must be a data frame or raw value")
}
