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
}
