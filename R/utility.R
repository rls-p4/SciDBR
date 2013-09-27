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

# This file contains general utility routines including most of the shim
# network interface.

# An environment to hold connection state
.scidbenv = new.env()

# An internal convenience function that conditionally evaluates a scidb
# query string `expr` (eval=TRUE), returning a scidb object,
# or returns a scidbexpr object (eval=FALSE).
# Optionally set lastclass to retain class information for evaluated result.
`scidbeval` = function(expr,eval,lastclass=c("scidb","scidbdf"))
{
  lastclass = match.arg(lastclass)
  if(`eval`)
  {
    newarray = tmpnam()
    query = sprintf("store(%s,%s)",expr,newarray)
    scidbquery(query)
    if(lastclass=="scidb") return(scidb(newarray,gc=TRUE))
    return(scidb(newarray,gc=TRUE,`data.frame`=TRUE))
  }
  scidbexpr(expr, lastclass=lastclass)
}

checkclass = function(x)
{
  if("scidbexpr" %in% class(x)) return(x@lastclass)
  class(x)[[1]]
}

# Return TRUE if our parent function lives in the scidb namespace, otherwise
# FALSE. This function is used to automatically control deferred evaluation
# of SciDB query expressions. If not nested, return FALSE.
# nf is the number of the frame of the calling function, we use it to locate
# our position on the frame stack. This is very substantially complicated
# by methods, for which I don't yet have a great solution.
called_from_scidb = function(nf=1)
{
  if(sys.nframe()<3 || nf==1) return(FALSE)
  f = sys.function(nf-1)
# We look for a scidb namespace, and handle a few special methods with a hack.
# XXX Is there a better way to do this? (There must be!)
  ans = grepl("namespace:scidb",capture.output(environment(f))[[1]])
  ans = ans || grepl("^sort",sys.call(nf-1)[[1]])
  ans = ans || grepl("^unique",sys.call(nf-1)[[1]])
  ans = ans || grepl("^subset",sys.call(nf-1)[[1]])
  ans = ans || grepl("^aggregate",sys.call(nf-1)[[1]])
  ans = ans || grepl("^merge",sys.call(nf-1)[[1]])
  ans
}

# store the connection information and obtain a unique ID
scidbconnect = function(host='localhost', port=8080L)
{
  assign("host",host, envir=.scidbenv)
  assign("port",port, envir=.scidbenv)
# Use the query ID from a bogus query as a unique ID for automated
# array name generation.
  x = scidbquery(query="load_library('dense_linear_algebra')",release=1,resp=TRUE)
  id = strsplit(x$response, split="\\r\\n")[[1]]
  id = id[[length(id)]]
  assign("uid",id,envir=.scidbenv)
# Set the ASCII interface precision
  scidbquery(query="setopt('precision','16')",release=1,resp=FALSE)
# Try to load the example_udos library (>= SciDB 13.6)
  scidbquery(query="load_library('example_udos')",release=1,resp=FALSE)
# Try to load the P4 library
  scidbquery(query="load_library('linear_algebra')",release=1,resp=FALSE)
# Save available operators
  assign("ops",iquery("list('operators')",return=TRUE),envir=.scidbenv)
  invisible()
}

scidbdisconnect = function()
{
  rm("host", envir=.scidbenv)
  rm("port", envir=.scidbenv)
  gc()
}

make.names_ = function(x)
{
  gsub("\\.","_",make.names(x, unique=TRUE),perl=TRUE)
}

# x is vector of existing values
# y is vector of new values
# returns a set the same size as y with non-conflicting value names
make.unique_ = function(x,y)
{
  z = scidb:::make.names_(c(x,y))
  setdiff(union(x,z),x)
}

# Make a name from a prefix and a unique SciDB identifier.
tmpnam = function(prefix="R_array")
{
  salt = basename(tempfile(pattern=prefix))
  if(!exists("uid",envir=.scidbenv)) stop("Not connected...try scidbconnect")
  paste(salt,get("uid",envir=.scidbenv),sep="")
}

# Return a shim session ID or error
getSession = function()
{
  u = paste(URI(),"/new_session",sep="")
  session = getURI(url=u)
  if(length(session)<1) stop("SciDB http session error; are you connecting to a valid SciDB host?")
  session = gsub("\r","",session)
  session = gsub("\n","",session)
  session
}

URI = function(q="")
{
  if(!exists("host",envir=.scidbenv)) stop("Not connected...try scidbconnect")
  ans  = paste("http://",get("host",envir=.scidbenv),":",get("port",envir=.scidbenv),sep="")
  if(nchar(q)>0) ans = paste(ans,q,sep="/")
  ans
}

# Send an HTTP GET message
GET = function(uri, header=TRUE)
{
  ans = invisible()
  if(!exists("host",envir=.scidbenv)) stop("Not connected...try scidbconnect")
  host = get("host",envir=.scidbenv)
  port = get("port",envir=.scidbenv)
  uri = URLencode(uri)
  uri = gsub("\\+","%2B",uri,perl=TRUE)
  u = paste(URI(),uri,sep="")
  ans = getURI(url=u, .opts=list(header=header))
  return(ans)
}


# Check if array exists
.scidbexists = function(name)
{
  Q = scidblist()
  return(name %in% Q)
}

# scidblist: A convenience wrapper for list().
# Input:
# type (character), one of the indicated list types
# verbose (boolean), include attribute and dimension data when type="arrays"
# n: maximum lines of output to return
# Output:
# A list.
scidblist = function(pattern,
type= c("arrays","operators","functions","types","aggregates","instances","queries"),
              verbose=FALSE, n=Inf)
{
  type = match.arg(type)
  Q = iquery(paste("list('",type,"')",sep=""), return=TRUE, n=n)

  if(dim(Q)[1]==0) return(NULL)
  z=Q[,-1,drop=FALSE]
  if(type=="arrays" && !verbose) {
    z=z[,1]
    if(!missing(pattern))
      z = grep(z,pattern=pattern,value=TRUE)
  }
  if(type=="arrays" && verbose) z=z[,3]
  z
}
scidbls = function(...) scidblist(...)

# Basic low-level query. Returns query id.
# query: a character query string
# afl: TRUE indicates use AFL, FALSE AQL
# async: TRUE=Ignore return value and return immediately, FALSE=wait for return
# save: Save format query string or NULL. If async=FALSE, save is ignored.
# release: Set to zero preserve web session until manually calling release_session
# session: if you already have a SciDB http session, set this to it, otherwise NULL
# resp(logical): return http response
# Example values of save:
# save="&save='dcsv'"
# save="&save='lcsv+'"
#
# Returns the HTTP session in each case
scidbquery = function(query, afl=TRUE, async=FALSE, save=NULL, release=1, session=NULL, resp=FALSE)
{
  DEBUG = FALSE
  if(!is.null(options("scidb.debug")[[1]]) && TRUE==options("scidb.debug")[[1]]) DEBUG=TRUE
  sessionid=session
  if(is.null(session))
  {
# Obtain a session from shim
    sessionid = getSession()
  }
  if(is.null(save)) save=""
  if(DEBUG)
  {
    cat(query, "\n")
    t1=proc.time()
  }
  if(async)
  {
    ans =tryCatch(
      GET(sprintf("/execute_query?id=%s&release=%d&query=%s",sessionid,release,query)),
      error=function(e) {
        GET(paste("/release_session?id=",sessionid,sep=""))
        stop("HTTP/1.0 500 ERROR")
      })
  } else
  {
    u = paste("/execute_query?id=",sessionid,"&release=",release,save,"&query=",query,"&afl=",as.integer(afl),sep="")
    ans = tryCatch(
      GET(u),
      error=function(e) {
        GET(paste("/release_session?id=",sessionid,sep=""))
        "HTTP/1.0 500 ERROR"
      })
    w = ans
    err = 200
    if(nchar(w)>9)
      err = as.integer(strsplit(substr(w,1,20)," ")[[1]][[2]])
    if(err>399)
    {
      w = paste(strsplit(w,"\r\n\r\n")[[1]][-1],collapse="\n")
      stop(w)
    }
  }
  if(DEBUG) print(proc.time()-t1)
  if(resp) return(list(session=sessionid, response=ans))
  sessionid
}

# scidbremove: Convenience function to remove one or more scidb arrays
# Input:
# x (character): a vector or single character string listing array names
# error (function): error handler. Use stop or warn, for example.
# Output:
# null
scidbremove = function(x, error=warning)
{
  if(inherits(x,"scidb")) x = x@name
  if(!inherits(x,"character")) stop("Invalid argument. Perhaps you meant to quote the variable name(s)?")
  for(y in x) {
    tryCatch( scidbquery(paste("remove(",y,")",sep=""),async=FALSE, release=1),
              error=function(e) error(e))
  }
  invisible()
}
scidbrm = function(x,error=warning) scidbremove(x,error)

# df2scidb: User function to send a data frame to SciDB
# Returns a scidbdf object
df2scidb = function(X,
                    name=tmpnam(),
                    dimlabel="row",
                    chunkSize,
                    rowOverlap=0L,
                    types=NULL,
                    nullable=FALSE,
                    schema_only=FALSE,
                    gc)
{
  if(!is.data.frame(X)) stop("X must be a data frame")
  if(missing(gc)) gc=FALSE
  if(length(nullable)==1) nullable = rep(nullable, ncol(X))
  if(length(nullable)!=ncol(X)) stop ("nullable must be either of length 1 or ncol(X)")
  if(!is.null(types) && length(types)!=ncol(X)) stop("types must match the number of columns of X")
  n1 = nullable
  nullable = rep("",ncol(X))
  if(any(n1)) nullable[n1] = "NULL"
  anames = make.names(names(X),unique=TRUE)
  anames = gsub("\\.","_",anames,perl=TRUE)
  if(length(anames)!=ncol(X)) anames=make.names(1:ncol(X))
  if(!all(anames==names(X))) warning("Attribute names have been changed")
# Check for attribute/dimension name conflict
  old_dimlabel = dimlabel
  dimlabel = tail(make.names(c(anames,dimlabel),unique=TRUE),n=1)
  dimlabel = gsub("\\.","_",dimlabel,perl=TRUE)
  if(dimlabel!=old_dimlabel) warning("Dimension name has been changed")
  if(missing(chunkSize)) {
    chunkSize = min(nrow(X),10000)
  }
  m = ceiling(nrow(X) / chunkSize)

# Default type is string
  typ = rep(paste("string",nullable),ncol(X))
  args = "<"
  if(!is.null(types)) {
    for(j in 1:ncol(X)) typ[j]=paste(types[j],nullable[j])
  } else {
    for(j in 1:ncol(X)) {
      if(class(X[,j])=="numeric") 
        typ[j] = paste("double",nullable[j])
      else if(class(X[,j])=="integer") 
        typ[j] = paste("int32",nullable[j])
      else if(class(X[,j])=="logical") 
        typ[j] = paste("bool",nullable[j])
      else if(class(X[,j]) %in% c("character","factor")) 
        typ[j] = paste("string",nullable[j])
    }  
  }
  for(j in 1:ncol(X)) {
    args = paste(args,anames[j],":",typ[j],sep="")
    if(j<ncol(X)) args=paste(args,",",sep="")
    else args=paste(args,">")
  }

  SCHEMA = paste(args,"[",dimlabel,"=1:",sprintf("%.0f",nrow(X)),",",sprintf("%.0f",chunkSize),",", rowOverlap,"]",sep="")

  if(schema_only) return(SCHEMA)

# Obtain a session from the SciDB http service for the upload process
  session = getSession()
  on.exit(GET(paste("/release_session?id=",session,sep="")) ,add=TRUE)

# Create SciDB input string from the data frame
  scidbInput = .df2scidb(X,chunkSize)

# Post the input string to the SciDB http service
  uri = paste(URI(),"/upload_file?id=",session,sep="")
  tmp = postForm(uri=uri, uploadedfile=fileUpload(contents=scidbInput,filename="scidb",contentType="application/octet-stream"),.opts=curlOptions(httpheader=c(Expect="")))
  tmp = tmp[[1]]
  tmp = gsub("\r","",tmp)
  tmp = gsub("\n","",tmp)

# Load query
  query = sprintf("store(input(%s, '%s'),%s)",SCHEMA, tmp, name)
  scidbquery(query, async=FALSE, release=1, session=session)
  scidb(name,`data.frame`=TRUE,gc=gc)
}

# Sparse matrix to SciDB
.Matrix2scidb = function(X,name,rowChunkSize=1000,colChunkSize=1000,start=c(0,0),gc=TRUE,...)
{
  D = dim(X)
  N = nnzero(X)
  rowOverlap=0L
  colOverlap=0L
  if(length(start)<1) stop ("Invalid starting coordinates")
  if(length(start)>2) start = start[1:2]
  if(length(start)<2) start = c(start, 0)
  start = as.integer(start)
  type = .scidbtypes[[typeof(X@x)]]
  if(is.null(type)) {
    stop(paste("Unupported data type. The package presently supports: ",
       paste(.scidbtypes,collapse=" "),".",sep=""))
  }
  if(type!="double") stop("Sorry, the package only supports double-precision sparse matrices right now.")
  schema = sprintf(
      "< val : %s >  [i=%.0f:%.0f,%.0f,%.0f, j=%.0f:%.0f,%.0f,%.0f]", type, start[[1]],
      nrow(X)-1+start[[1]], min(nrow(X),rowChunkSize), rowOverlap, start[[2]], ncol(X)-1+start[[2]],
      min(ncol(X),colChunkSize), colOverlap)
  schema1d = sprintf("<i:int64, j:int64, val : %s>[idx=0:*,100000,0]",type)
# Obtain a session from shim for the upload process
  session = getSession()
  if(length(session)<1) stop("SciDB http session error")
  on.exit( GET(paste("/release_session?id=",session,sep="")) ,add=TRUE)

# Compute the indices and assemble message to SciDB in the form
# double,double,double for indices i,j and data val.
  dp = diff(X@p)
  j  = rep(seq_along(dp),dp) - 1
# Upload the data
# XXX I couldn't get RCurl to work using the fileUpload(contents=x), with 'x'
# a raw vector. But we need RCurl to support SSL. As a work-around, we save
# the object. This copy sucks and must be fixed.
  fn = tempfile()
  bytes = writeBin(as.vector(t(matrix(c(X@i + start[[1]],j + start[[2]], X@x),length(X@x)))),con=fn)
  url = paste(URI(),"/upload_file?id=",session,sep="")
  ans = postForm(uri = url, uploadedfile = fileUpload(filename=fn),
                 .opts = curlOptions(httpheader = c(Expect = "")))
  unlink(fn)
  ans = ans[[1]]
  ans = gsub("\r","",ans)
  ans = gsub("\n","",ans)

  query = sprintf("store(redimension(input(%s,'%s',0,'(double,double,double)'),%s),%s)",schema1d, ans, schema, name)
  iquery(query)
  scidb(name,gc=gc)
}


iquery = function(query, `return`=FALSE,
                  afl=TRUE, iterative=FALSE,
                  n=1000, excludecol, ...)
{
  if(!afl && `return`) stop("return=TRUE may only be used with AFL statements")
  if(iterative && !`return`) stop("Iterative result requires return=TRUE")
  if(missing(excludecol)) excludecol=NA
  if(iterative)
  {
    sessionid = scidbquery(query,afl,async=FALSE,save="&save=lcsv+",release=0)
    return(iqiter(con=sessionid,n=n,excludecol=excludecol,...))
  }
  qsplit = strsplit(query,";")[[1]]
  m = 1
  if(n==Inf) n = -1    # Indicate to shim that we want all the lines of output
  for(query in qsplit)
  {
    if(`return` && m==length(qsplit))
    {
      ans = tryCatch(
       {
        sessionid = scidbquery(query,afl,async=FALSE,save="&save=lcsv+",release=0)
        u = sprintf("/read_lines?id=%s&n=%.0f",sessionid,n+1)
        val = textConnection(GET(u, header=FALSE))
        ret=read.table(val,sep=",",stringsAsFactors=FALSE,header=TRUE,...)
        close(val)
        GET(paste("/release_session?id=",sessionid,sep=""))
        ret
       }, error = function(e)
           {
             stop(e)
           })
    } else ans = scidbquery(query,afl)
    m = m + 1
  }
  if(!(`return`)) return(invisible())
  ans
}

iqiter = function (con, n = 1, excludecol, ...)
{
  if(missing(excludecol)) excludecol=NA
  dostop = function(s=TRUE)
  {
    GET(paste("/release_session?id=",con,sep=""))
    if(s) stop("StopIteration",call.=FALSE)
  }
  if (!is.numeric(n) || length(n) != 1 || n < 1)
    stop("n must be a numeric value >= 1")
  init = TRUE
  header = c()
  nextEl = function() {
    if (is.null(con)) dostop()
    u = sprintf("/read_lines?id=%s&n=%.0f",con,n)
    if(init) {
      ans = tryCatch(
       {
        val = textConnection(GET(u, header=FALSE))
        ret=read.table(val,sep=",",stringsAsFactors=FALSE,header=TRUE,nrows=n,...)
        close(val)
       }, error = function(e) {dostop()},
          warning = function(w) {dostop()}
      )
      header <<- colnames(ans)
      init <<- FALSE
    } else {
      ans = tryCatch(
       {
        val = textConnection(GET(u, header=FALSE))
        ret = read.table(val,sep=",",stringsAsFactors=FALSE,header=TRUE,nrows=n...)
        close(val)
       }, error = function(e) {dostop()},
          warning = function(w) {dostop()}
      )
      colnames(ans) = header
    }
    if(!is.na(excludecol) && excludecol<=ncol(ans))
    {
      rownames(ans) = ans[,excludecol]
      ans=ans[,-excludecol, drop=FALSE]
    }
    ans
  }
  it = list(nextElem = nextEl, gc=new.env())
  class(it) = c("abstractiter", "iter")
  it$gc$remove = TRUE
  reg.finalizer(it$gc, function(e) if(e$remove) 
                  tryCatch(dostop(FALSE),error=function(e) stop(e)),
                  onexit=TRUE)
  it
}


# Utility csv2scidb function
.df2scidb = function(X, chunksize, start=1)
{
  if(missing(chunksize)) chunksize = min(nrow(X),10000L)
#  scipen = options("scipen")
#  options(scipen=20)
#  buf = capture.output(
#         write.table(X, file=stdout(), sep=",",
#                     row.names=FALSE,col.names=FALSE,quote=FALSE)
#        )
#  options(scipen=scipen)
#  x = sapply(1:length(buf), function(j)
#    {
#      chunk = floor(j/chunksize)
#      if(j==length(buf))
#      {
#        if((j-1) %% chunksize == 0)
#          tmp = sprintf("{%.0f}[\n(%s)];",start + chunk*chunksize, buf[j])
#        else
#          tmp = sprintf("(%s)];",buf[j])
#      } else if((j-1) %% chunksize==0)
#      {
#        tmp = sprintf("{%.0f}[\n(%s),",start + chunk*chunksize, buf[j])
#      } else if((j) %% chunksize == 0)
#      {
#        tmp = sprintf("(%s)];",buf[j])
#      } else
#      {
#        tmp = sprintf("(%s),",buf[j])
#      }
#      tmp
#    }
#  )
#  x = paste(x,collapse="")
#  ans = paste(x,collapse="")

  ans = .Call("df2scidb",X,as.integer(chunksize), as.double(round(start)), "%.15f")
  ans
}

# Return a SciDB schema of a scidb object x.
# Explicitly indicate attribute part of schema with remaining arguments
`extract_schema` = function(x, at=x@attributes, ty=x@types, nu=x@nullable)
{
  if(!(inherits(x,"scidb") || inherits(x,"scidbdf"))) stop("Not a scidb object")
  op = options(scipen=20)
  nullable = rep("",length(nu))
  if(any(nu)) nullable[nu] = " NULL"
  attr = paste(at,ty,sep=":")
  attr = paste(attr, nullable,sep="")
  attr = sprintf("<%s>",paste(attr,collapse=","))
  dims = sprintf("[%s]",paste(paste(paste(paste(paste(paste(x@D$name,"=",sep=""),x@D$start,sep=""),x@D$start+x@D$length-1,sep=":"),x@D$chunk_interval,sep=","),x@D$chunk_overlap,sep=","),collapse=","))
  options(op)
  paste(attr,dims,sep="")
}

# Build the attibute part of a SciDB array schema from a scidb,
# scidbdf, or scidbexpr object.
`build_attr_schema` = function(A)
{
  if("scidbexpr" %in% class(A)) A = scidb_from_scidbexpr(A)
  if(!(class(A) %in% c("scidb","scidbdf"))) stop("Invalid SciDB object")
  N = rep("",length(A@nullable))
  N[A@nullable] = " NULL"
  N = paste(A@types,N,sep="")
  S = paste(paste(A@attributes,N,sep=":"),collapse=",")
  sprintf("<%s>",S)
}

`noE` = function(w) sapply(w, function(x) sprintf("%.0f",x))

# Build the dimension part of a SciDB array schema from a scidb,
# scidbdf, or scidbexpr object.
`build_dim_schema` = function(A,bracket=TRUE)
{
  if("scidbexpr" %in% class(A)) A = scidb_from_scidbexpr(A)
  if(!(class(A) %in% c("scidb","scidbdf"))) stop("Invalid SciDB object")
  notint = A@D$type != "int64"
  N = rep("",length(A@D$name))
  N[notint] = paste("(",A@D$type,")",sep="")
  N = paste(A@D$name, N,sep="")
  low = noE(A@D$low)
  high = noE(A@D$high)
  if(is.na(A@D$low))
    low = noE(A@D$start)
  if(is.na(A@D$high))
  {
    high = noE(A@D$start + A@D$length - 1)
  }
  R = paste(low,high,sep=":")
  R[notint] = noE(A@D$length)
  S = paste(N,R,sep="=")
  S = paste(S,noE(A@D$chunk_interval),sep=",")
  S = paste(S,noE(A@D$chunk_overlap),sep=",")
  S = paste(S,collapse=",")
  if(bracket) S = sprintf("[%s]",S)
  S
}

# .scidbdim is an internal function that retirieves dimension metadata from a
# scidb array called "name."
.scidbdim = function(name)
{
#  if(!.scidbexists(name)) stop ("not found") 
  d = iquery(paste("dimensions(",name,")"),return=TRUE)
# R is unfortunately interpreting 'i' as an imaginary unit I think.
  if(any(is.na(d))) d[is.na(d)] = "i"
  d
}

# Retrieve list of attributes for a named SciDB array (internal function).
.scidbattributes = function(name)
{
  x = iquery(paste("attributes(",name,")",sep=""),return=TRUE,colClasses=c(NA,"character",NA,NA))
# R is unfortunately interpreting 'i' as an imaginary unit I think.
  if(any(is.na(x))) x[is.na(x)] = "i"
  list(attributes=x[,2],types=x[,3],nullable=(x[,4]=="true"))
}
