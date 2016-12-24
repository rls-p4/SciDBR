# Functions for parsing and building SciDB schema strings.
# A SciDB schema string looks like:
# <attribute_1:type_1 NULL DEFAULT VALUE, attribute_2:type_2, ...>
# [dimension_1=start:end,chunksize,overlap, dimension_2=start:end,chunksize,overlap, ...]

.dimsplitter = function(x)
{
  if(!(inherits(x, "scidb"))) return(NULL)
  if(is.character(x)) s = x
  else
  {
    s = schema(x)
  }
  d = gsub("\\]", "", strsplit(s, "\\[")[[1]][[2]])
  d = strsplit(strsplit(d, "=")[[1]], ",")
  # SciDB schema syntax changed greatly in 16.9, convert it to old format.
  if(newer_than(attr(x@meta$db, "connection")$scidb.version, "16.9"))
  { 
    d = lapply(d, function(x)  strsplit(gsub(";[ ]", ",", gsub("(.*):(.*):(.*):(.*$)", "\\1:\\2,\\4,\\3", x)), ",")[[1]])
  }
  n = c(d[[1]], vapply(d[-c(1, length(d))], function(x) x[length(x)], ""))
  d = d[-1]
  if(length(d) > 1)
  {
    i = 1:(length(d) - 1)
    d[i] = lapply(d[i], function(x) x[-length(x)])
  }
  d = lapply(d, function(x) c(strsplit(x[1], ":")[[1]], x[-1]))
  data.frame(name=n,
             start=vapply(d, function(x) x[1], ""),
             end=vapply(d, function(x) x[2], ""),
             chunk=vapply(d, function(x) x[3], ""),
             overlap=vapply(d, function(x) x[4], ""), stringsAsFactors=FALSE)
}

.attsplitter = function(x)
{
  if(is.character(x)) s = x
  else
  {
    if(!(inherits(x, "scidb"))) return(NULL)
    s = schema(x)
  }
  s = strsplit(strsplit(strsplit(strsplit(s, ">")[[1]][1], "<")[[1]][2], ",")[[1]], ":")
  # SciDB schema syntax changed in 15.12
  null = if(newer_than(attr(x@meta$db, "connection")$scidb.version, "15.12"))
           ! grepl("NOT NULL", s)
         else grepl(" NULL", s)
  type = gsub(" ", "", gsub("null", "", gsub("not null", "", vapply(s, function(x) x[2], ""), ignore.case=TRUE), ignore.case=TRUE))
  data.frame(name=vapply(s, function(x) x[1], ""),
             type=type,
             nullable=null, stringsAsFactors=FALSE)
}



#' SciDB array schema
#' @param x a \code{\link{scidb}} array object
#' @param what optional schema subset (subsets are returned in data frames; partial
#'  argument matching is supported)
#' @return character-valued SciDB array schema
#' @examples
#' \dontrun{
#' s <- scidbconnect()
#' x <- scidb(s,"build(<v:double>[i=1:10,2,0,j=0:19,1,0],0)")
#' schema(x)
#' # [1] "<v:double> [i=1:10:0:2; j=0:19:0:1]"
#' schema(x, "attributes")
#' #  name   type nullable
#' #1    v double     TRUE
#' schema(x, "dimensions")
#'   name start end chunk overlap
#' #1    i     1  10     2       j
#' #2    0     0  19     1       0
#' }
#' @export
schema = function(x, what=c("schema", "attributes", "dimensions"))
{
  if(!(inherits(x, "scidb"))) return(NULL)
  switch(match.arg(what),
    schema = gsub(".*<", "<", x@meta$schema),
    attributes = .attsplitter(x),
    dimensions = .dimsplitter(x),
    invisible()
  )
}

dfschema = function(names, types, len, chunk=10000)
{
  sprintf("<%s>[i=1:%d,%d,0]", paste(paste(names, types, sep=":"), collapse=","), len, chunk)
}
