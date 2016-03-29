# Miscellaneous tests

check = function(a,b)
{
  print(match.call())
  stopifnot(all.equal(a,b,check.attributes=FALSE,check.names=FALSE))
}

library("scidb")
host = Sys.getenv("SCIDB_TEST_HOST")
if(nchar(host)>0)
{
  scidbconnect(host)
  options(scidb.debug=TRUE)

# Upload/download binary blobs
  data("iris")
  x = as.scidb(serialize(iris, NULL))
  y = unserialize(x[][[1]][[1]])
  stopifnot(isTRUE(all.equal(iris, y)))

# Misc.
  x = as.scidb(iris)
  check(nrow(unique(project(x, "Species"))[]), 3)
  check( nrow(xgrid(regrid(x, 10, "avg(Petal_Length) as Petal_Length"), 10, "max(Petal_Length)")),
         150)

# issue 81
  x = scidb_from_schemastring("<v:double>[i=1:2,1,0],1")
  check(is.scidb(x), TRUE)
}
