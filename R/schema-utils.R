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
  d
}

.attsplitter = function(x)
{
  if(is.character(x)) s = x
  else
  {
    if(!(inherits(x, "scidb"))) return(NULL)
    s = schema(x)
  }
  strsplit(strsplit(strsplit(strsplit(s, ">")[[1]][1], "<")[[1]][2], ",")[[1]], ":")
}

#' SciDB array attribute names
#' @param x a \code{\link{scidb}} array object
#' @return character vector of SciDB attribute names.
#' @export
scidb_attributes = function(x)
{
  a = .attsplitter(x)
  unlist(lapply(a, function(x) x[[1]]))
}

#' SciDB array attribute types
#' @param x a \code{\link{scidb}} array object
#' @return character vector of SciDB attribute types
scidb_types = function(x)
{
  a = .attsplitter(x)
  unlist(lapply(a, function(x) strsplit(x[2]," ")[[1]][1]))
}

#' SciDB array attribute nullability
#' @param x a \code{\link{scidb}} array object
#' @return logical vector of SciDB attribute nullability
scidb_nullable = function(x)
{
  # SciDB schema syntax changed in 15.12
  if(newer_than(attr(x@meta$db, "connection")$scidb.version, "15.12"))
  { 
    return (! grepl("NOT NULL", .attsplitter(x)))
  }
  grepl(" NULL", .attsplitter(x))
}

#' SciDB array dimension names
#' @param x a \code{\link{scidb}} array object
#' @return character vector of SciDB dimension names
#' @export
dimensions = function(x)
{
  d = .dimsplitter(x)
#  gsub("^ *","",unlist(lapply(d[-length(d)],function(x) x[[length(x)]])))
  h = paste(gsub("^ *","",d[[1]]), collapse="|")
  d = d[-1]
  if(length(d)>1)
  {
    h = c(h,gsub("^ *","",unlist(lapply(d[-length(d)],function(x) paste(x[4:length(x)],collapse="|")))))
  }
  h
}

#' SciDB array coordinate bounds
#' @param x a \code{\link{scidb}} array object
#' @return list of character-valued vectors of starting and ending coordinate bounds
#' @export
scidb_coordinate_bounds = function(x)
{
  d = .dimsplitter(x)
  start = unlist(lapply(d[-1],function(x)strsplit(x[1],":")[[1]][1]))
  end = unlist(lapply(d[-1],function(x)strsplit(x[1],":")[[1]][2]))
  start = gsub("NA","0",start)
  end = gsub("NA","\\*",end)
  s1 = gsub("\\*",.scidb_DIM_MAX,start)
  s2 = gsub("\\*",.scidb_DIM_MAX,end)
  len = as.numeric(s2) - as.numeric(s1) + 1
  i = len >= as.double(.scidb_DIM_MAX)
  len = noE(len) # in particular, len is now character
  if(any(i))
  {
    len[i] = "Inf"
  }
  list(start=noE(start), end=noE(end), length=len)
}

#' SciDB array coordinate chunksize
#' @param x a \code{\link{scidb}} array object
#' @return character-valued vector of SciDB coordinate chunk sizes
#' @export
scidb_coordinate_chunksize = function(x)
{
  d = .dimsplitter(x)
  unlist(lapply(d[-1], function(x) x[2]))
}

#' SciDB array coordinate overlap
#' @param x a \code{\link{scidb}} array object
#' @return character-valued vector of SciDB coordinate overlap
#' @export
scidb_coordinate_overlap = function(x)
{
  d = .dimsplitter(x)
  unlist(lapply(d[-1], function(x) x[3]))
}

#' SciDB array schema
#' @param x a \code{\link{scidb}} array object
#' @return character-valued SciDB array schema
#' @export
schema = function(x)
{
  if(!(inherits(x, "scidb"))) return(NULL)
  gsub(".*<", "<", x@meta$schema)
}

# A utility function for operations that require a single attribute
# Throws error if a multi-attribute array is specified.
.get_attribute = function(x)
{
  a = scidb_attributes(x)
  if(length(a) > 1) stop("This function requires a single-attribute array. Consider using project.")
  a
}


dfschema = function(names, types, len, chunk=10000)
{
  sprintf("<%s>[i=1:%d,%d,0]", paste(paste(names, types, sep=":"), collapse=","), len, chunk)
}
