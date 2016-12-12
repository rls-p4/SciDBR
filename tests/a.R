check = function(a, b)
{
  print(match.call())
  stopifnot(all.equal(a, b, check.attributes=FALSE, check.names=FALSE))
}

library("scidb")
host = Sys.getenv("SCIDB_TEST_HOST")
if(nchar(host) > 0)
{
  db = scidbconnect(host)
  options(scidb.debug=TRUE)

# upload
  x = as.scidb(db, iris)
# binary download
# iquery binary download
# iquery CSV download

# garbage collection
  rm(x)
  gc()
}
