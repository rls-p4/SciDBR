# Non-exported utility functions

# An environment to hold connection state
.scidbenv = new.env()

# Map scidb object column classes into R, adding an extra integer at the start for the index!
scidbcc = function(x)
{
  if(!is.null(options("scidb.test")[[1]]))
  {
    cat("Using old method for data.frame import")
    return(NA)
  }
  c("integer",as.vector(unlist(lapply(.scidbtypes[scidb_types(x)],function(x) ifelse(is.null(x),NA,x)))))
}

.scidbstr = function(object)
{
  name = substr(object@name,1,35)
  if(nchar(object@name)>35) name = paste(name,"...",sep="")
  dn = "\nDimension"
  if(length(dimensions(object))>1) dn = "\nDimensions"
  cat("SciDB expression ",name)
  cat("\nSciDB schema ",schema(object))
  cat(dn,"\n")
  bounds = scidb_coordinate_bounds(object)
  cat(paste(capture.output(print(data.frame(dimension=dimensions(object),start=bounds$start,end=bounds$end,chunk=scidb_coordinate_chunksize(object),row.names=NULL,stringsAsFactors=FALSE))),collapse="\n"))
  cat("\nAttributes\n")
  cat(paste(capture.output(print(data.frame(attribute=object@attributes,type=scidb_types(object),nullable=scidb_nullable(object)))),collapse="\n"))
  cat("\n")
}


#' Unpack and return a SciDB query expression as a data frame
#' @param query A SciDB query expression or scidb object
#' @param ... optional extra arguments
#' @keywords internal
#' @importFrom curl new_handle handle_setheaders handle_setopt curl_fetch_memory handle_setform form_file
scidb_unpack_to_dataframe = function(query, ...)
{
  DEBUG = FALSE
  projected = FALSE
  if(!inherits(query, "scidb")) query = scidb(query)
  if(!is.null(options("scidb.debug")[[1]]) && TRUE==options("scidb.debug")[[1]]) DEBUG=TRUE
  buffer = 100000L
  args = list(...)
  if(!is.null(args$buffer))
  {
    argsbuf = tryCatch(as.integer(args$buffer),warning=function(e) NA)
    if(!is.na(argsbuf) && argsbuf < 100e6) buffer = as.integer(argsbuf)
  }
  dims = paste(paste(dimensions(query), dimensions(query), sep=","), collapse=",") # Note! much faster than unpack
  ndim = length(dimensions(query))
  x = scidb(sprintf("apply(%s, %s)", query@name, dims))
  N = scidb_nullable(x)
  TYPES = scidb_types(x)
  ns = rep("",length(N))
  ns[N] = "null"
  format_string = paste(paste(TYPES,ns),collapse=",")
  format_string = sprintf("(%s)",format_string)
  sessionid = scidbquery(x@name, save=format_string, release=0)
  on.exit( SGET("/release_session",list(id=sessionid), err=FALSE) ,add=TRUE)

  dt2 = proc.time()
  uri = URI("/read_bytes",list(id=sessionid,n=0))
  h = new_handle()
  handle_setheaders(h, .list=list(`Authorization`=digest_auth("GET", uri)))
  handle_setopt(h, .list=list(ssl_verifyhost=as.integer(options("scidb.verifyhost")),
                              ssl_verifypeer=0))
  resp = curl_fetch_memory(uri, h)
  if(resp$status_code > 299) stop("HTTP error", resp$status_code)
# Explicitly reap the handle to avoid short-term build up of socket descriptors
  rm(h)
  gc()
  if(DEBUG) cat("Data transfer time",(proc.time()-dt2)[3],"\n")
  dt1 = proc.time()
  len = length(resp$content)
  p = 0
  ans = c()
  cnames = c(scidb_attributes(x),"lines","p")  # we are unpacking to a SciDB array, ignore dims
  n = length(scidb_attributes(x))
  rnames = c()
  if(projected) n = length(args$project)
  while(p < len)
  {
    dt2 = proc.time()
    tmp   = .Call("scidb_parse", as.integer(buffer), TYPES, N, resp$content, as.double(p), PACKAGE="scidb")
    names(tmp) = cnames
    lines = tmp[[n+1]]
    p_old = p
    p     = tmp[[n+2]]
    if(DEBUG) cat("  R buffer ",p,"/",len," bytes parsing time",(proc.time() - dt2)[3],"\n")
    dt2 = proc.time()
    if(lines>0)
    {
      if("binary" %in% TYPES)
      {
        if(DEBUG) cat("  R rbind/df assembly time",(proc.time() - dt2)[3],"\n")
        return(lapply(1:n, function(j) tmp[[j]][1:lines]))
      }
      len_out = length(tmp[[1]])
      if(lines < len_out) tmp = lapply(tmp[1:n],function(x) x[1:lines])
# Let's adaptively re-estimate a buffer size
      avg_bytes_per_line = ceiling((p - p_old)/lines)
      buffer = ceiling(1.3*(len - p)/avg_bytes_per_line) # Engineering factor
# Assemble the data frame
      if(is.null(ans)) ans = data.frame(tmp[1:n], stringsAsFactors=FALSE)
      else ans = rbind(ans, data.frame(tmp[1:n], stringsAsFactors=FALSE))
    }
    if(DEBUG) cat("  R rbind/df assembly time",(proc.time()-dt2)[3],"\n")
  }
  dt2 = proc.time()
# reorder so that dimensions appear to the left
  n = ncol(ans)
  if(n > 0)
  {
    na = 1:(n - ndim)
    nd = (n - ndim + 1):n
    ans = ans[, c(nd, na)]
  }
  if(DEBUG) cat("Total R parsing time",(proc.time()-dt1)[3],"\n")
  ans
}

#' Convenience function for digest authentication.
#' @param method digest method
#' @param uri uri
#' @param realm realm
#' @param nonce nonce
#' @keywords internal
#' @importFrom digest digest
digest_auth = function(method, uri, realm="", nonce="123456")
{
  if(exists("authtype",envir=.scidbenv))
  {
   if(.scidbenv$authtype != "digest") return(NULL)
  }
  uri = gsub(".*/","/",uri)
  userpwd = .scidbenv$digest
  if(is.null(userpwd)) userpwd=":"
  up = strsplit(userpwd,":")[[1]]
  user = up[1]
  pwd  = up[2]
  if(is.na(pwd)) pwd=""
  ha1=digest(sprintf("%s:%s:%s", user, realm, pwd, algo="md5"), serialize=FALSE)
  ha2=digest(sprintf("%s:%s", method,  uri, algo="md5"), serialize=FALSE)
  cnonce="MDc1YmFhOWFkY2M0YWY2MDAwMDBlY2JhMDAwMmYxNTI="
  nc="00000001"
  qop="auth"
  response=digest(sprintf("%s:%s:%s:%s:%s:%s", ha1, nonce, nc, cnonce, qop, ha2), algo="md5", serialize=FALSE)
  sprintf('Digest username="%s", realm=%s, nonce="%s", uri="%s", cnonce="%s", nc=%s, qop=%s, response="%s"', user, realm, nonce, uri, cnonce, nc, qop, response)
}

# Internal warning function
warnonce = (function() {
  state = list(
    count="Use `count` for an exact count of data elements.",
    nonum="Note: The R sparse Matrix package does not support certain value types like\ncharacter strings",
    toobig="Array coordinates are too big or infinite! Returning data in unpacked data.frame form, or try using `bound`."
  )
  function(warn) {
    if(!is.null(state[warn][[1]])) {
      message(state[warn])
      s <<- state
      s[warn] = c()
      state <<- s
    }
  }
}) ()


# Utility function that parses an aggregation expression
# makes sense of statements like
# max(a) as amax, avg(b) as bmean, ...
# Input
# x: scidb object
# expr: aggregation expression (as is required)
# Output
# New attribute schema
aparser = function(x, expr)
{
  map = list(
    ApproxDC    = function(x) "double null",
    avg         = function(x) "double null",
    count       = function(x) "uint64 null",
    first_value = function(x) sprintf("%s null",x),
    last_value  = function(x) sprintf("%s null",x),
    mad         = function(x) sprintf("%s null",x),
    max         = function(x) sprintf("%s null",x),
    min         = function(x) sprintf("%s null",x),
    median      = function(x) sprintf("%s null",x),
    prod        = function(x) sprintf("%s null",x),
    stdev       = function(x) "double null",
    sum         = function(x) sprintf("%s null",x),
    top_five    = function(x) sprintf("%s null",x),
    var         = function(x) "double null")

  p = strsplit(expr,",")[[1]]                      # comma separated
  p = lapply(p,function(v) strsplit(v,"as")[[1]])  # as is required
  new_attrs = lapply(p, function(v) v[[2]])        # output attribute names
  y = lapply(p,function(v)strsplit(v,"\\(")[[1]])  #
  fun = lapply(y, function(v) gsub(" ","",v[[1]])) # functions
  old_attrs = unlist(lapply(y,function(v)gsub("[\\) ]","",v[[2]])))
  i = 1:length(x@attributes)
  names(i)=x@attributes
  old_types = scidb_types(x)[i[old_attrs]]
  old_types[is.na(old_types)] = "int64"    # catch all
  z = rep("",length(new_attrs))
  for(j in 1:length(z))
  {
    z[j] = sprintf("%s:%s", new_attrs[j],map[unlist(fun)][[j]](old_types[j]))
  }
  sprintf("<%s>", paste(z, collapse=","))
}

# Some versions of RCurl seem to contain a broken URLencode function.
oldURLencode = function (URL, reserved = FALSE) 
{
    OK = paste0("[^-ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz0123456789$_.+!*'(),", 
        if (!reserved) 
            ";/?:@=&", "]")
    x = strsplit(URL, "")[[1L]]
    z = grep(OK, x)
    if (length(z)) {
        y = sapply(x[z], function(x) paste0("%", as.character(charToRaw(x)), 
            collapse = ""))
        x[z] = y
    }
    paste(x, collapse = "")
}


#' Nightmarish internal function that converts R expressions to SciDB expressions
#' @param expr an R 'language' type object to parse
#' @param sci a SciDB array 
#' @keywords internal
#' @return character-valued SciDB expression
#' @importFrom codetools makeCodeWalker walkCode
rewrite_subset_expression = function(expr, sci)
{
  dims = dimensions(sci)
  n = length(dims)
  template = rep("",2*n)
  DEBUG = FALSE
  if(!is.null(options("scidb.debug")[[1]]) && TRUE==options("scidb.debug")[[1]]) DEBUG=TRUE

  .toList = makeCodeWalker(call=function(e, w) lapply(e, walkCode, w),
                           leaf=function(e, w) e)

# Walk the ast for R expression x, annotating elements with identifying
# attributes. Specify a character vector of array dimension in dims.
# Substitute evaluated R scalars for variables where possible (but not more
# complext R expressions).  The output is a list that can be parsed by the
# `.compose_r` function below.
  .annotate = function(x, dims=NULL, attr=NULL, frames, op="")
  {
    if(is.list(x))
    {
      if(length(x)>1) op = c(op,as.character(x[[1]]))
      return(lapply(x, .annotate, dims, attr, frames, op))
    }
    op = paste(op,collapse="")
    s = as.character(x)
    if(!(s %in% c(dims,attr,">","<","!","|","=","&","||","&&","!=","==","<=",">=")))
    {
      test = lapply(c(globalenv(),frames), function(f)  # perhaps overkill
      {
        tryCatch(eval(x,f), error=function(e) e)
      })
      if(length(test)>0)
      {
        test = test[!grepl("condition",lapply(test,class))]
        if(length(test)>0)
        {
          if(DEBUG) cat("Replacing symbol",s,"with")
          s = tryCatch(as.character(test[[length(test)]]), error=function(e) s)
          if(DEBUG) cat(s,"\n")
        }
      }
    }
    attr(s,"what") = "element"
    if("character" %in% class(x)) attr(s,"what") = "character"
    if(nchar(gsub("null","",gsub("[0-9 \\-\\.]+","",s),ignore.case=TRUE))==0)
      attr(s,"what") = "scalar"
    if(any(dims %in% gsub(" ","",s)) && nchar(gsub("[&(<>=) ]*","",op))==0)
    {
      attr(s,"what") = "dimension"
    }
    s
  }

  .compose_r = function(x)
  {
    if(is.list(x))
    {
      if(length(x)==3)
      {
        if(!is.list(x[[2]]) && !is.list(x[[3]]) &&
           all( c("scalar","dimension") %in%
                c(attr(x[[2]],"what"), attr(x[[3]],"what"))))
        {
          b = template
          d = which(c(attr(x[[2]],"what"), attr(x[[3]],"what")) == "dimension") + 1
          s = which(c(attr(x[[2]],"what"), attr(x[[3]],"what")) == "scalar") + 1
          i = which(dims %in% x[[d]]) # template index
          intx = round(as.numeric(x[[s]]))
          if(intx >= 2^53) stop("Values too large, use an explicit SciDB filter expression")
          numx = as.numeric(x[[s]])
          lb = ceiling(numx)
          ub = floor(intx)
          if(intx==numx)
          {
            lb = intx + 1
            ub = intx - 1
          }
          if(x[[1]] == "==" || x[[1]] == "=") b[i] = b[i+n] = x[[s]]
          else if(x[[1]] == "<") b[i+n] = sprintf("%.0f",ub)
          else if(x[[1]] == "<=") b[i+n] = sprintf("%.0f",intx)
          else if(x[[1]] == ">") b[i] = sprintf("%.0f",lb)
          else if(x[[1]] == ">=") b[i] = sprintf("%.0f",intx)
          b = paste(b,collapse=",")
          return(sprintf("::%s",b))
        }
        return(c(.compose_r(x[[2]]), as.character(x[[1]]), .compose_r(x[[3]])))
      }
      if(length(x)==2)
      {
        if(as.character(x[[1]])=="(") return(c(.compose_r(x[[1]]),.compose_r(x[[2]]), ")"))
        return(c(.compose_r(x[[1]]),.compose_r(x[[2]])))
      }
    }
    if(attr(x,"what")=="character") return(sprintf("'%s'",as.character(x)))
    as.character(x)
  }

  ans = .compose_r(.annotate(walkCode(expr, .toList), dims=dims, attr=scidb_attributes(sci), frames=sys.frames()))
  i   = grepl("::",ans)
  ans = gsub("==","=",gsub("!","not",gsub("\\|","or",gsub("\\|\\|","or", gsub("&","and",gsub("&&","and",gsub("!=","<>",ans)))))))
  if(any(i))
  {
# Compose the betweens in a highly non-elegant way
    b = strsplit(paste(gsub("::","",ans[i]),""),",")
    ans[i] = "true"
    b = Reduce(function(x,y)
    {
      n = length(x)
      x = tryCatch(as.numeric(x), warning=function(e) rep(NA,n))
      y = tryCatch(as.numeric(y), warning=function(e) rep(NA,n))
      m = n/2
      x1 = x[1:m]
      y1 = y[1:m]
      x1[is.na(x1)] = -Inf
      y1[is.na(y1)] = -Inf
      x1 = pmax(x1,y1)
      x2 = x[(m+1):n]
      y2 = y[(m+1):n]
      x2[is.na(x2)] = Inf
      y2[is.na(y2)] = Inf
      x2 = pmin(x2,y2)
      c(x1,x2)
    }, b, init=template)
    b = gsub("Inf","null",gsub("-Inf","null",as.character(b)))
    ans = gsub("and true","",paste(ans,collapse=" "))
    if(nchar(gsub(" ","",ans))==0 || gsub(" ","",ans)=="true")
      return(sprintf("between(%s,%s)",sci@name,paste(b,collapse=",")))
    return(sprintf("filter(between(%s,%s),%s)",sci@name,paste(b,collapse=","),ans))
  }
  sprintf("filter(%s,%s)",sci@name,paste(ans,collapse=" "))
}


# Internal function
create_temp_array = function(name, schema)
{
# SciDB temporary array syntax varies with SciDB version
  TEMP = "'TEMP'"
  if(compare_versions(options("scidb.version")[[1]],14.12)) TEMP="true"
  query   = sprintf("create_array(%s, %s, %s)", name, schema, TEMP)
  iquery(query, `return`=FALSE)
}


# An important internal convenience function that returns a scidb object.  If
# eval=TRUE, a new SciDB array is created the returned scidb object refers to
# that.  Otherwise, the returned scidb object represents a SciDB array promise.
#
# INPUT
# expr: (character) A SciDB expression or array name
# eval: (logical) If TRUE evaluate expression and assign to new SciDB array.
#                 If FALSE, infer output schema but don't evaluate.
# name: (optional character) If supplied, name for stored array when eval=TRUE
# gc: (optional logical) If TRUE, tie SciDB object to  garbage collector.
# depend: (optional list) An optional list of other scidb or scidb objects
#         that this expression depends on (preventing their garbage collection
#         if other references to them go away).
# schema, temp: (optional) used to create SciDB temp arrays
#               (requires scidb >= 14.8)
#
# OUTPUT
# A `scidb` array object.
#
# NOTE
# Only AFL supported.
`.scidbeval` = function(expr, eval=FALSE, name, gc=TRUE, depend, schema, temp)
{
  ans = c()
  if(missing(depend)) depend = c()
  if(missing(schema)) schema = ""
  if(missing(temp)) temp = FALSE
  if(!is.list(depend)) depend = list(depend)
# Address bug #45. Try to cheaply determine if expr refers to a named array
# or an AFL expression. If it's a named array, then eval must be set TRUE.
  if(!grepl("\\(", expr, perl=TRUE)) eval = TRUE
  if(`eval`)
  {
    if(missing(name) || is.null(name))
    {
      newarray = tmpnam()
      if(temp) create_temp_array(newarray, schema)
    }
    else newarray = name
    query = sprintf("store(%s,%s)", expr, newarray)
    scidbquery(query, stream=0L)
    ans = scidb(newarray, gc=gc)
    if(temp) ans@gc$temp = TRUE
# This is a fix for a SciDB issue that can unexpectedly change schema
# bounds. And another fix to allow unexpected dimname and attribute name
# changes. Arrgh.
    if(schema != "" && !compare_schema(ans, schema, ignore_attributes=TRUE, ignore_dimnames=TRUE))
    {
      ans = repart(ans, schema)
    }
  } else
  {
    ans = scidb(expr, gc=gc)
# Assign dependencies
    if(length(depend) > 0)
    {
      assign("depend", depend, envir=ans@gc)
    }
  }
  ans
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
  z = make.names(gsub("_",".",c(x,y)),unique=TRUE)
  gsub("\\.","_",tail(z,length(y)))
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
  session = SGET("/new_session")
  if(length(session)<1) stop("SciDB http session error; are you connecting to a valid SciDB host?")
  session = gsub("\r","",session)
  session = gsub("\n","",session)
  session
}

# Supply the base SciDB URI from the global host, port and auth
# parameters stored in the .scidbenv package environment.
# Every function that needs to talk to the shim interface should use
# this function to supply the URI.
# Arguments:
# resource (string): A URI identifying the requested service
# args (list): A list of named query parameters
URI = function(resource="", args=list())
{
  if(!exists("host",envir=.scidbenv)) stop("Not connected...try scidbconnect")
  if(exists("auth",envir=.scidbenv))
    args = c(args,list(auth=get("auth",envir=.scidbenv)))
  prot = paste(get("protocol",envir=.scidbenv),"//",sep=":")
  if("username" %in% names(args) || "auth" %in% names(args)) prot = "https://"
  ans  = paste(prot, get("host",envir=.scidbenv),":",get("port",envir=.scidbenv),sep="")
  ans = paste(ans, resource, sep="/")
  if(length(args)>0)
    ans = paste(ans,paste(paste(names(args),args,sep="="),collapse="&"),sep="?")
  ans
}

SGET = function(resource, args=list(), err=TRUE, binary=FALSE)
{
  if(!(substr(resource,1,1)=="/")) resource = paste("/", resource, sep="")
  uri = URI(resource, args)
  uri = oldURLencode(uri)
  uri = gsub("\\+","%2B", uri, perl=TRUE)
  h = new_handle()
  handle_setheaders(h, .list=list(Authorization=digest_auth("GET", uri)))
  handle_setopt(h, .list=list(ssl_verifyhost=as.integer(options("scidb.verifyhost")),
                              ssl_verifypeer=0))
  ans = curl_fetch_memory(uri, h)
  if(ans$status_code > 299 && err)
  {
    msg = sprintf("HTTP error %s", ans$status_code)
    if(ans$status_code >= 500) msg = sprintf("%s\n%s", msg, rawToChar(ans$content))
    stop(msg)
  }
  if(binary) return(ans$content)
  rawToChar(ans$content)
}

#' Basic HTTP GET request
#' @param url a well-formed HTTP URL, will be url-encoded
#' @return R raw binary HTTP content field
#' @export
GET_RAW = function(url)
{
  uri = oldURLencode(url)
  h = new_handle()
  handle_setheaders(h, .list=list(Authorization=digest_auth("GET", uri)))
  handle_setopt(h, .list=list(ssl_verifyhost=as.integer(options("scidb.verifyhost")),
                              ssl_verifypeer=0))
  ans = curl_fetch_memory(uri, h)
  if(ans$status_code > 299)
  {
    msg = sprintf("HTTP error %s", ans$status_code)
    if(ans$status_code >= 500) msg = sprintf("%s\n%s", msg, rawToChar(ans$content))
    stop(msg)
  }
  ans$content
}

# Normally called with raw data and args=list(id=whatever)
POST = function(data, args=list(), err=TRUE)
{
# check for new shim simple post option (/upload), otherwise use
# multipart/file upload (/upload_file)
  shimspl = strsplit(options("shim.version")[[1]],"\\.")[[1]]
  shim_yr = as.integer(gsub("[A-z]","",shimspl[1]))
  shim_mo = as.integer(gsub("[A-z]","",shimspl[2]))
  if(is.na(shim_yr)) shim_yr = 14
  if(is.na(shim_mo)) shim_mo = 1
  simple = (shim_yr >= 15 && shim_mo >= 7) || shim_yr >= 16
  if(simple)
  {
    uri = URI("/upload", args)
    uri = oldURLencode(uri)
    uri = gsub("\\+","%2B", uri, perl=TRUE)
    h = new_handle()
    handle_setheaders(h, .list=list(Authorization=digest_auth("POST", uri)))
    handle_setopt(h, .list=list(ssl_verifyhost=as.integer(options("scidb.verifyhost")),
                                ssl_verifypeer=0, post=TRUE, postfieldsize=length(data), postfields=data))
    ans = curl_fetch_memory(uri, h)
    if(ans$status_code > 299 && err) stop("HTTP error ", ans$status_code)
    return(rawToChar(ans$content))
  }
  uri = URI("/upload_file", args)
  uri = oldURLencode(uri)
  uri = gsub("\\+","%2B", uri, perl=TRUE)
  h = new_handle()
  handle_setheaders(h, .list=list(Authorization=digest_auth("POST", uri)))
  handle_setopt(h, .list=list(ssl_verifyhost=as.integer(options("scidb.verifyhost")),
                              ssl_verifypeer=0))
  tmpf = tempfile()
  if(is.character(data)) data = charToRaw(data)
  writeBin(data, tmpf)
  handle_setform(h, file=form_file(tmpf))
  ans = curl_fetch_memory(uri, h)
  unlink(tmpf)
  if(ans$status_code > 299 && err) stop("HTTP error", ans$status_code)
  return(rawToChar(ans$content))
}

CACHE = function(data, args=list(), err=TRUE)
{
  uri = URI("/cache", args)
  uri = oldURLencode(uri)
  uri = gsub("\\+","%2B", uri, perl=TRUE)
  h = new_handle()
  handle_setheaders(h, .list=list(Authorization=digest_auth("POST", uri)))
  handle_setopt(h, .list=list(ssl_verifyhost=as.integer(options("scidb.verifyhost")),
                              ssl_verifypeer=0, post=TRUE, postfieldsize=length(data), postfields=data))
  ans = curl_fetch_memory(uri, h)
  if(ans$status_code > 299 && err) stop("HTTP error ", ans$status_code)
  rawToChar(ans$content)
}

# Check if array exists
.scidbexists = function(name)
{
  Q = scidblist()
  return(name %in% Q)
}

# Basic low-level query. Returns query id. This is an internal function.
# query: a character query string
# save: Save format query string or NULL.
# release: Set to zero preserve web session until manually calling release_session
# session: if you already have a SciDB http session, set this to it, otherwise NULL
# resp(logical): return http response
# stream: Set to 0L or 1L to control streaming, otherwise use package options
# Example values of save:
# save="dcsv"
# save="csv+"
# save="(double NULL, int32)"
#
# Returns the HTTP session in each case
scidbquery = function(query, save=NULL, release=1, session=NULL, resp=FALSE, stream)
{
  DEBUG = FALSE
  STREAM = 0L
  if(!is.null(options("scidb.debug")[[1]]) && TRUE==options("scidb.debug")[[1]]) DEBUG=TRUE
  if(missing(stream))
  {
    if(!is.null(options("scidb.stream")[[1]]) && TRUE==options("scidb.stream")[[1]]) STREAM=1L
  } else STREAM = as.integer(stream)
  sessionid = session
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
  ans = tryCatch(
    {
      if(is.null(save))
        SGET("/execute_query", list(id=sessionid, release=release,
             query=query, afl=0L, stream=0L))
      else
        SGET("/execute_query", list(id=sessionid,release=release,
            save=save, query=query, afl=0L, stream=STREAM))
    }, error=function(e)
    {
# User cancel?
      SGET("/cancel", list(id=sessionid), err=FALSE)
      SGET("/release_session", list(id=sessionid), err=FALSE)
      stop(as.character(e))
    }, interrupt=function(e)
    {
      SGET("/cancel", list(id=sessionid), err=FALSE)
      SGET("/release_session", list(id=sessionid), err=FALSE)
      stop("cancelled")
    })
  if(DEBUG) cat("Query time",(proc.time()-t1)[3],"\n")
  if(resp) return(list(session=sessionid, response=ans))
  sessionid
}

# Sparse matrix to SciDB
.Matrix2scidb = function(X,name,rowChunkSize=1000,colChunkSize=1000,start=c(0,0),gc=TRUE,...)
{
  D = dim(X)
  N = Matrix::nnzero(X)
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


# raw value to special 1-element SciDB array
raw2scidb = function(X, name, gc=TRUE, ...)
{
  if(!is.raw(X)) stop("X must be a raw value")
  args = list(...)
# Obtain a session from shim for the upload process
  session = getSession()
  if(length(session)<1) stop("SciDB http session error")
  on.exit(SGET("/release_session", list(id=session), err=FALSE) ,add=TRUE)

  bytes = .Call("scidb_raw", X, PACKAGE="scidb")
  ans = POST(bytes, list(id=session))
  ans = gsub("\n", "", gsub("\r", "", ans))

  schema = "<val:binary null>[i=0:0,1,0]"
  if(!is.null(args$temp))
  {
    if(args$temp) create_temp_array(name, schema)
  }

  query = sprintf("store(input(%s,'%s',-2,'(binary null)'),%s)",schema, ans, name)
  iquery(query)
  scidb(name,gc=gc)
}

# Check for scidb missing flag
is.nullable = function(x)
{
  any(scidb_nullable(x))
}

# Internal utility function, make every attribute of an array nullable
make_nullable = function(x)
{
  cast(x,sprintf("%s%s",build_attr_schema(x,nullable=TRUE),build_dim_schema(x)))
}

# Internal utility function used to format numbers
noE = function(w) sapply(w,
  function(x)
  {
    if(is.infinite(x)) return("*")
    if(is.character(x)) return(x)
    sprintf("%.0f",x)
  })
# Returns TRUE if version string x is greater than or equal to than version y
compare_versions = function(x,y)
{
  b = as.numeric(strsplit(as.character(x),"\\.")[[1]])
  a = as.numeric(strsplit(as.character(y),"\\.")[[1]])
  ans = b[1] > a[1]
  if(b[1] == a[1]) ans = b[2] >= a[2]
  ans
}

