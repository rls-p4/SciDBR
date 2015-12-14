#
#    _____      _ ____  ____
#   / ___/_____(_) __ \/ __ )
#   \__ \/ ___/ / / / / __  |
#  ___/ / /__/ / /_/ / /_/ / 
# /____/\___/_/_____/_____/  
#
#
#
# BEGIN_COPYRIGHT
#
# This file is part of SciDB.
# Copyright (C) 2008-2014 SciDB, Inc.
#
# SciDB is free software: you can redistribute it and/or modify
# it under the terms of the AFFERO GNU General Public License as published by
# the Free Software Foundation.
#
# SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
# INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
# NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
# the AFFERO GNU General Public License for the complete license terms.
#
# You should have received a copy of the AFFERO GNU General Public License
# along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
#
# END_COPYRIGHT
#

#' Evaluate an Expression to \code{scidb} or \code{scidbdf} objects
#'
#' Force evaluation of an expression that yields a \code{scidb} or \code{scidbdf} object,
#' storing the result to a SciDB array when \code{eval=TRUE}.
#' @param exp A SciDB expression or \code{scidb} or \code{scidbdf} object
#' @param name (character) optional SciDB array name to store as
#' @param gc (logical) optional, when TRUE tie result to R garbage collector
#' @param temp (logical, optional): when TRUE store as a SciDB temp array
#' @export
scidbeval = function(expr, eval=TRUE, name, gc=TRUE, temp=FALSE)
{
  ans = eval(expr)
  if(!(inherits(ans,"scidb") || inherits(ans,"scidbdf"))) return(ans)
# If expr is a stored temp array, then re-use its name
  if(!is.null(ans@gc$temp) && ans@gc$temp && missing(name)) name=ans@name
  .scidbeval(ans@name, `eval`=eval, name=name, gc=gc, schema=ans@schema, temp=temp)
}

# Create a new scidb reference to an existing SciDB array.
# name (character): SciDB expression or array name
# gc (logical, optional): Remove backing SciDB array when R object is
#     garbage collected? Default is FALSE.
# data.frame (logical, optional): If true, return a data.frame-like object.
#   Otherwise an array.
scidb = function(name, gc, `data.frame`)
{
  if(missing(name)) stop("array or expression must be specified")
  if(missing(gc)) gc=FALSE
  if(is.scidb(name) || is.scidbdf(name))
  {
    query = name@name
    return(.scidbeval(name@name, eval=FALSE, gc=gc, `data.frame`=`data.frame`, depend=list(name)))
  }
  escape = gsub("'","\\\\'",name,perl=TRUE)
# SciDB explain_logical operator changed in version 15.7
  if(compare_versions(options("scidb.version")[[1]],15.7))
  {
    query = sprintf("join(show('filter(%s,true)','afl'), _explain_logical('filter(%s,true)','afl'))", escape,escape)
  }
  else
  {
    query = sprintf("join(show('filter(%s,true)','afl'), explain_logical('filter(%s,true)','afl'))", escape,escape)
  }
  query = iquery(query, `return`=TRUE, binary=FALSE)
# XXX NOTE that we need binary=FALSE here to avoid a terrible recursion
  logical_plan = query$logical_plan
  schema = gsub("^.*<","<",query$schema, perl=TRUE)
  obj = scidb_from_schemastring(schema, name, `data.frame`)
  obj@logical_plan = logical_plan
  if(gc)
  {
    if(length(grep("\\(",name))==0)
    {
      obj@gc$name = name
    }
    obj@gc$remove = TRUE
    reg.finalizer(obj@gc, function(e)
        {
          if (e$remove && exists("name",envir=e))
            {
              tryCatch(scidbremove(e$name,warn=FALSE), error = function(e) invisible())
            }
        }, onexit = TRUE)
  } else obj@gc = new.env()
  obj
}

# TRUE if the SciDB object 'name' or character name of a SciDB array is
# a temporary SciDB array, FALSE otherwise.
is.temp = function(name)
{
  if(is.scidb(name) || is.scidbdf(name))
  {
    if(!is.null(name@gc$temp)) return(name@gc$temp)
    name = name@name
  }
  query = sprintf("filter(list('arrays'),name='%s')", name)
  ans = iquery(query,return=TRUE)
  if(nrow(ans)<1) return(FALSE)
  if(is.null(ans$temporary)) return(FALSE)
  ans$temporary == "true"
}


# store the connection information and obtain a unique ID
scidbconnect = function(host=options("scidb.default_shim_host")[[1]],
                        port=options("scidb.default_shim_port")[[1]],
                        username, password,
                        auth_type=c("scidb","digest"), protocol=c("http","https"))
{
  scidbdisconnect()
  auth_type = match.arg(auth_type)
  protocol = match.arg(protocol)
  assign("host",host, envir=.scidbenv)
  assign("port",port, envir=.scidbenv)
  assign("protocol",protocol,envir=.scidbenv)
  if(missing(username)) username=c()
  if(missing(password)) password=c()
# Check for login using either scidb or HTTP digest authentication
  if(!is.null(username))
  {
    assign("authtype",auth_type,envir=.scidbenv)
    assign("authenv",new.env(),envir=.scidbenv)
    if(auth_type=="scidb")
    {
      auth = paste(username, password, sep=":")
      assign("auth",auth,envir=.scidbenv)
    } else # HTTP basic digest auth
    {
      assign("digest",paste(username,password,sep=":"),envir=.scidbenv)
    }
  }

# Use the query ID from a query as a unique ID for automated
# array name generation.
  x = tryCatch(
        scidbquery(query="setopt('precision','16')",release=1,resp=TRUE,stream=0L),
        error=function(e) stop("Connection error"))
  if(is.null(.scidbenv$uid))
  {
    id = tryCatch(strsplit(x$response, split="\\r\\n")[[1]],
           error=function(e) stop("Connection error"))
    id = id[[length(id)]]
    assign("uid",id,envir=.scidbenv)
  }
# Try to load the accelerated_io_tools, then load_tools, then
# prototype_load_tools libraries:
  got_load = tryCatch(
    {
      scidbquery(query="load_library('accelerated_io_tools')",
               release=1,resp=FALSE, stream=0L)
      TRUE
    }, error=function(e) FALSE)
  if(!got_load) got_load = tryCatch(
    {
      scidbquery(query="load_library('load_tools')",
               release=1,resp=FALSE, stream=0L)
      TRUE
    }, error=function(e) FALSE)
  if(!got_load) got_load = tryCatch(
    {
      scidbquery(query="load_library('prototype_load_tools')",
               release=1,resp=FALSE, stream=0L)
      TRUE
    }, error=function(e) FALSE)
  if(!got_load) warning("The load_tools SciDB plugin can't be found. load_tools is required to upload data.frames from R to SciDB. You can install the plugin from https://github.com/Paradigm4/load_tools")
# Try to load the dense_linear_algebra library
  tryCatch(
    scidbquery(query="load_library('dense_linear_algebra')",
               release=1,resp=FALSE, stream=0L),
    error=invisible)
# Try to load the example_udos library (>= SciDB 13.6)
  tryCatch(
    scidbquery(query="load_library('example_udos')",release=1,resp=FALSE,stream=0L),
    error=invisible)
# Try to load the superfunpack
  tryCatch(
    scidbquery(query="load_library('superfunpack')",release=1,resp=FALSE,stream=0L),
    error=invisible)
# Try to load the P4 library
  tryCatch(
    scidbquery(query="load_library('linear_algebra')",release=1,resp=FALSE,stream=0L),
    error=invisible)
# Try to load the chunk_unique library
  tryCatch(
    scidbquery(query="load_library('cu')",release=1,resp=FALSE,stream=0L),
    error=invisible)
# Save available operators
  assign("ops",iquery("list('operators')",`return`=TRUE,binary=FALSE),envir=.scidbenv)
# Update the scidb.version option
  v = scidbls(type="libraries")[1,]
  if("major" %in% names(v))
  {
    options(scidb.version=paste(v$major,v$minor,sep="."))
  }
# Update the gemm_bug option
  if(compare_versions(options("scidb.version")[[1]],14.3))
  {
    options(scidb.gemm_bug=FALSE) # Yay
  }
# Update the shim.version option
  options(shim.version=GET("/version"))

  invisible()
}

scidbdisconnect = function()
{
# forces call to scidblogout via finalizer, see scidbconnect:
  if(exists("authenv",envir=.scidbenv)) rm("authenv",envir=.scidbenv)
  gc()
  rm(list=ls(envir=.scidbenv), envir=.scidbenv)
}


#' A convenience wrapper for SciDB list().
#' @param type (character), one of the indicated list types
#' @param  verbose (boolean), include attribute and dimension data when type="arrays"
#' @param n maximum lines of output to return
#' @return a list.
#' @export
scidblist = function(pattern,
                     type= c("arrays", "operators", "functions", "types",
                             "aggregates", "instances", "queries", "libraries"),
                     verbose=FALSE, n=Inf)
{
  type = match.arg(type)
  Q = iquery(paste("list('",type,"')",sep=""), return=TRUE, n=n, binary=FALSE)

  if(dim(Q)[1]==0) return(NULL)
  z=Q[,-1,drop=FALSE]
  if(type=="arrays" && !verbose) {
    z=z[,1]
    if(!missing(pattern))
      z = grep(z,pattern=pattern,value=TRUE)
  }
  z
}
scidbls = function(...) scidblist(...)


# scidbremove: Convenience function to remove one or more scidb arrays
# Input:
# x (character): a vector or single character string listing array names
# error (function): error handler. Use stop or warn, for example.
# async (optional boolean): If TRUE use expermental shim async option for speed
# force (optional boolean): If TRUE really remove this array, even if scidb.safe_remove=TRUE
# recursive (optional boolean): If TRUE, recursively remove this array and its dependency graph
# Output:
# null
scidbremove = function(x, error=warning, async, force, warn=TRUE, recursive=FALSE) UseMethod("scidbremove")
scidbremove.default = function(x, error=warning, async, force, warn=TRUE, recursive=FALSE)
{
  if(is.null(x)) return(invisible())
  if(missing(async)) async=FALSE
  if(missing(force)) force=FALSE
  errfun=error

  safe = options("scidb.safe_remove")[[1]]
  if(is.null(safe)) safe = TRUE
  if(!safe) force=TRUE
  uid = get("uid",envir=.scidbenv)
  if(is.scidb(x) || is.scidbdf(x)) x = list(x)
  for(y in x)
  {
# Depth-first, a bad way to traverse this XXX improve
    if(recursive && (is.scidb(y) || is.scidbdf(y)) && !is.null(unlist(y@gc$depend)))
    {
      for(z in y@gc$depend) scidbremove(z,error,async,force,warn,recursive)
    }
    if(is.scidb(y) || is.scidbdf(y)) y = y@name
    if(grepl("\\(",y)) next  # Not a stored array
    query = sprintf("remove(%s)",y)
    if(grepl(sprintf("^R_array.*%s$",uid),y,perl=TRUE))
    {
      tryCatch( scidbquery(query,async=async, release=1, stream=0L),
                error=function(e) if(!recursive && warn)errfun(e))
    } else if(force)
    {
      tryCatch( scidbquery(query,async=async, release=1, stream=0L),
                error=function(e) if(!recursive && warn)errfun(e))
    } else if(warn)
    {
      warning("The array ",y," is protected from easy removal. Specify force=TRUE if you really want to remove it.")
    }
  }
  invisible()
}
scidbrm = function(x,error=warning,...) scidbremove(x,error,...)

# df2scidb: User function to send a data frame to SciDB
# Returns a scidbdf object
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
  if(missing(start)) start=1
  start = as.numeric(start)
  if(missing(gc)) gc=FALSE
  if(!missing(nullable)) warning("The nullable option has been deprecated. All uploaded attributes will be nullable by default. Use the `replaceNA` function to change this.")
  nullable = TRUE
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
  chunkSize = as.numeric(chunkSize)
  m = ceiling(nrow(X) / chunkSize)

# Default type is string
  typ = rep("string",ncol(X))
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
  query = sprintf("store(redimension(%s,%s),%s)",LOAD, SCHEMA, name)
  scidbquery(query, async=FALSE, release=1, session=session, stream=0L)
  scidb(name,`data.frame`=TRUE,gc=gc)
}

# binary=FALSE is needed by some queries, don't get rid of it.
#' Run a SciDB query, optionally returning the result.
#' @param query a single SciDB query string
#' @param return if \code{TRUE}, return the result
#' @param afl \code{TRUE} for AFL queries, required for now (AQL not yet supported)
#' @param n maximum number of bytes/lines to return
#' @param binary set to \code{FALSE} to read result from SciDB in text form
#' @param ... additional options passed to \code{read.table} when \code{binary=FALSE}
#' @export
iquery = function(query, `return`=FALSE,
                  afl=TRUE, n=Inf, binary=TRUE, ...)
{
  DEBUG = FALSE
  if(!is.null(options("scidb.debug")[[1]]) && TRUE==options("scidb.debug")[[1]]) DEBUG=TRUE
  if(!afl && `return`) stop("return=TRUE may only be used with AFL statements")
  if(is.scidb(query) || is.scidbdf(query))  query=query@name
  qsplit = strsplit(query,";")[[1]]
  m = 1
  if(n==Inf) n = -1    # Indicate to shim that we want all the output
  for(query in qsplit)
  {
# Only return the last query, mimicking command-line iquery.
    if(`return` && m==length(qsplit))
    {
      if(binary)
      {
        return(scidb_unpack_to_dataframe(query,...))
      }
      ans = tryCatch(
       {
        sessionid = scidbquery(query,afl,async=FALSE,save="lcsv+",release=0)
        dt1 = proc.time()
        result = tryCatch(
          {
            GET("/read_lines", list(id=sessionid, n=as.integer(n+1)))
          },
          error=function(e)
          {
             GET("/cancel", list(id=sessionid))
             GET("/release_session", list(id=sessionid), err=FALSE)
             stop(e)
          })
        GET("/release_session", list(id=sessionid), err=FALSE)
        if(DEBUG) cat("Data transfer time",(proc.time()-dt1)[3],"\n")
        dt1 = proc.time()
# Handle escaped quotes
        result = gsub("\\\\'","''",result, perl=TRUE)
        result = gsub("\\\\\"","''",result, perl=TRUE)
# Map SciDB missing (aka null) to NA, but preserve DEFAULT null.
# This sucks, need to avoid this parsing and move on to binary xfer.
        result = gsub("DEFAULT null","@#@#@#kjlkjlkj@#@#@555namnsaqnmnqqqo",result,perl=TRUE)
        result = gsub("null","NA",result, perl=TRUE)
        result = gsub("@#@#@#kjlkjlkj@#@#@555namnsaqnmnqqqo","DEFAULT null",result,perl=TRUE)
        val = textConnection(result)
        ret = tryCatch({
                read.table(val,sep=",",stringsAsFactors=FALSE,header=TRUE,...)},
                error=function(e){ warning(e);c()})
        close(val)
        if(DEBUG) cat("R parsing time",(proc.time()-dt1)[3],"\n")
        ret
       }, error = function(e)
           {
             stop(e)
           })
    } else
    {
      ans = scidbquery(query, afl, async=FALSE, release=1, stream=0L)
    }
    m = m + 1
  }
  if(!(`return`)) return(invisible())
  ans
}

# translate array coordinate system
translate = function(x, origin="origin", newstart, newchunk)
{
  if(is.numeric(origin))
  {
    if(missing(newchunk)) newchunk = scidb_coordinate_chunksize(x)
    oldstart = as.numeric(scidb_coordinate_start(x))
    if(missing(newstart)) newstart = noE(origin)
    d = noE(origin - oldstart)
    dims = dimensions(x)
    new_indices = make.unique_(dims, rep("i",length(dims)))
    expr = paste(dims, d, sep="+")
    expr = paste(paste(new_indices,expr,sep=","), collapse=",")
    unend   = rep("*",length(newstart))
    newschema = sprintf("%s%s", build_attr_schema(x),
                  build_dim_schema(x, newnames=new_indices, newstart=newstart, newend=unend, newchunk=newchunk))
    query = sprintf("redimension(apply(%s, %s),%s)",x@name,expr,newschema)
    return(scidb(query))
  }
  else if(origin=="origin")
  {
    N = paste(rep("null",2*length(dimensions(x))),collapse=",")
    query = sprintf("subarray(%s,%s)",x@name,N)
    return(.scidbeval(query,`eval`=FALSE,depend=list(x),gc=TRUE))
  }
  else if(!is.scidb(origin)) stop("origin must be either 'origin', numeric coordinates, or another SciDB array")
# translate by same-named subset of dimensions between arrays
  stop("not implemented yet")
}

# Walk the dependency graph, setting all upstreams array to persist
# Define a generic persist
persist = function(x, remove=FALSE, ...) UseMethod("persist")
persist.default = function(x, remove=FALSE, ...)
{
  DEBUG = FALSE
  if(!is.null(options("scidb.debug")[[1]]) && TRUE==options("scidb.debug")[[1]]) DEBUG=TRUE
  if(!(is.scidb(x) || is.scidbdf(x))) return(invisible())
  if(DEBUG) cat("Persisting ",x@name,"\n")
  x@gc$remove = remove
  if(is.null(unlist(x@gc$depend))) return()
  for(y in x@gc$depend)
  {
    if(DEBUG) cat("Persisting ",y@name,"\n")
    y@gc$remove = remove
    if(!is.null(y@gc$depend)) persist(y, remove=remove)
  }
}

# A special persist function for complicated model objects
persist.glm_scidb = function(x, remove=FALSE, ...)
{
  .traverse.glm_scidb(x, persist, remove)
}
# A special remove function for complicated model objects
scidbremove.glm_scidb = function(x, error=warning, async=FALSE, force=FALSE, warn=TRUE, recursive=TRUE)
{
  .traverse.glm_scidb(x, scidbremove, error, async, force, warn, recursive)
}

# Show the repository log (not in namespace)
show_commit_log = function()
{
  log = system.file("misc/log",package="scidb")
  if(file.exists(log))
  {
    file.show(log)
  }
}
