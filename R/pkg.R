#' SciDB/R Interface
#'
#' @name scidb-package
#' 
#' @useDynLib scidb
#' @seealso \code{\link{scidb}}, \code{\link{iquery}}
#' @docType package
NULL

.onAttach = function(libname,pkgname)
{
  packageStartupMessage("   ____    _ ___  ___\n  / __/___(_) _ \\/ _ )\n _\\ \\/ __/ / // / _  |\n/___/\\__/_/____/____/     Copyright 2016, Paradigm4, Inc.\n\n"    , domain = NULL, appendLF = TRUE)

  options(scidb.prefix=NULL)
# Default shim port and host.
  options(scidb.default_shim_port=8080L)
  options(scidb.default_shim_host="localhost")
# Binary data parser buffer size
  options(scidb.buffer_size = 5e7)
# How to download arrays and their coordinates. Set scidb.unpack=FALSE
# to use apply, which can be faster in some cases when used with aio.
  options(scidb.unpack=FALSE)
# Disable SSL certificate host name checking by default. This is important mostly
# for Amazon EC2 where hostnames rarely match their DNS names. If you enable this
# then the shim SSL certificate CN entry *must* match the server host name for the
# encrypted session to work. Set this TRUE for stronger security (help avoid MTM)
# in SSL connections.
  options(scidb.verifyhost=FALSE)
# Set to TRUE to enable experimental shim stream protocol, avoids copying query
# output to data file on server # (see https://github.com/Paradigm4/shim).
# THIS MUST BE SET TO FALSE FOR VERSIONS OF SCIDB < 15.7.
  options(scidb.stream=FALSE)
}

# Reset the various package options
.onUnload = function(libpath)
{
  options(scidb.buffer_size=c())
  options(scidb.default_shim_port=c())
  options(scidb.default_shim_host=c())
  options(scidb.verifyhost=c())
  options(scidb.stream=c())
}

# scidb array object type map.
# R type -> SciDB type
.Rtypes = list(
  double="double",
  double="int64",
  double="uint64",
  integer="int32",
  logical="bool",
  character="string"
)

# These types are used to infer dataframe column classes.
# SciDB type -> R type
.scidbtypes = list(
  double="double",
  int64="double",
  uint64="double",
  uint32="double",
  int32="integer",
  int16="integer",
  unit16="integer",
  int8="integer",
  uint8="integer",
  bool="logical",
  string="character",
  char="character",
  datetime="Date"
)

.typelen = list(
  double=8,
  integer=4,
  logical=1,
  character=1
)

# SciDB Integer dimension minimum, maximum
.scidb_DIM_MIN = "-4611686018427387902"
.scidb_DIM_MAX = "4611686018427387903"

# To quiet a check NOTE:
if(getRversion() >= "2.15.1")  utils::globalVariables(c("n", "p"))
