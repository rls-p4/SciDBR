# 'scidb' object tests

check = function(a, b)
{
  print(match.call())
  stopifnot(all.equal(a, b, check.attributes=FALSE, check.names=FALSE))
}

library("scidb")
host = Sys.getenv("SCIDB_TEST_HOST")
if(nchar(host) > 0)
{
  scidbconnect(host)
  options(scidb.debug=TRUE)

# upload
  x = as.scidb(iris)
# binary download
  a = x[, drop=TRUE]  # less SciDB dimension
  lapply(1:4,  function(j) check(all.equal(iris[, j], a[, j]), TRUE))  # less factor column
  check(all.equal(as.character(iris[, 5]), a[, 5]), TRUE)  # character column
# iquery binary download
  a = iquery(x, return=TRUE, binary=TRUE)[, -seq(1, length(dimensions(x)))]
  lapply(1:4,  function(j) check(all.equal(iris[, j], a[, j]), TRUE))  # less factor column
  check(all.equal(as.character(iris[, 5]), a[, 5]), TRUE)  # character column
# iquery CSV download
  a = iquery(x, return=TRUE, binary=FALSE)[, -seq(1, length(dimensions(x)))]
  lapply(1:4,  function(j) check(all.equal(iris[, j], a[, j]), TRUE))  # less factor column
  check(all.equal(as.character(iris[, 5]), a[, 5]), TRUE)  # character column

# head
  a = head(x, 3)
  b = iqdf(x, 3, 1)
  c = iqdf(x, 10, 0.25)
  
# garbage collection
  rm(x)
  gc()

# misc schema-modifying ops
  x = as.scidb(iris)
  check(is.scidb(x), TRUE)
  check(length(dim(x)), 2)
  y = cast(x, schema(x))
  redimension(x, schema(x))
  y = transform(x, i="iif(Species='setosa', 1, iif(Species='versicolor', 2, 3))")
  z = redimension(y, dim=c(dimensions(y), "i"))
  check(length(dimensions(z)), length(dimensions(y)) + 1)

  repart(x, schema(x))
  repart(x, schema(x), '*', 2, 1) 
  reshape_scidb(x, schema(x))
  reshape_scidb(subarray(x, c(0, 0, 0, 0, 0, 149)), shape=150)

  unbound(x)
  replaceNA(x)

# join
  y = merge(x, x)
  check(length(scidb_attributes(y)), 10)
  y = merge(x, x, merge=TRUE)
  check(length(scidb_attributes(y)), 5)
  y = merge(x, z) # crossjoin
  check(length(scidb_attributes(y)), 10)

  antijoin(x, x)

  # join on attributes
  set.seed(1)
  a = as.scidb(data.frame(a=sample(10, 5), b=rnorm(5)), chunk_size=1000)
  b = as.scidb(data.frame(u=sample(10, 5), v=rnorm(5)), chunk_size=1000)
  # problem introduced by aio loader :(
  if("src_instance_id" %in% dimensions(a))
  {
    a = slice(a, c("dst_instance_id", "src_instance_id"), c(0,0))
    b = slice(b, c("dst_instance_id", "src_instance_id"), c(0,0))
  }
  merge(x=a, y=b, by.x="a", by.y="u")[]
  x = scidb("apply(build(<a:string>[i=0:5,2,0], '[(abc),(def),(ghi),(jkl),(mno)]', true), b, double(i)*1.1)")
  y = scidb("apply(build(<c:string>[j=1:5,3,0], '[(def),(mno),(pqr),(def)]', true), d, j)")
  check(count(merge(x, y, by.x='a', by.y='c')), 3)
  check(count(merge(x, y, by.x=c("a","i"), by.y=c("c", "d"))), 1)

  # outer join
  merge(x, x, all=TRUE)

# subset and NSE
  y = subset(x, "Species = 'setosa'")
  z = subset(x, Species == "setosa")
  check(count(y), count(z))
  i = 40
  if("tuple_no" %in% dimensions(x))
  {
    y = subset(x, "Species = 'setosa' and tuple_no > 40")
    z = subset(x, Species == 'setosa' & tuple_no > i)
    check(count(y), count(z))
  }
  else if("line_no" %in% dimensions(x)) # inconsistent naming in upload dimensions depending on SciDB load plugin :(
  {
    y = subset(x, "Species = 'setosa' and line_no > 40")
    z = subset(x, Species == 'setosa' & line_no > i)
    check(count(y), count(z))
  }


  # from issue #86
  iquery("create_array(genotype_test, <allele_1:bool,allele_2:bool,phase:bool> [chromosome_id=0:*,1,0,start=0:*,10000000,0,end=0:*,10000000,0,alternate_id=0:19,20,0,sample_id=0:*,100,0], 0)")
  genotype = scidb("genotype_test")
  chromosome = 7
  start_coord = 10000000
  end_coord = 40000000
  check(grepl("filter", subset(genotype, chromosome_id == (chromosome - 1) && end >= start_coord && start <= end_coord)@name), FALSE)
  check(grepl("filter", subset(genotype, chromosome_id == (chromosome - 1) && end >= start_coord && start <= end_coord && phase > 0)@name), TRUE)
  iquery("remove(genotype_test)")

# aggregation
  a = aggregate(x, by="Species", FUN=mean)   # by attribute
  y = as.scidb(data.frame(sample(1:4, 150, replace=TRUE)))
  # by auxilliary vector
  aggregate(x, by=y, FUN="avg(Petal_Width) as apw, min(Sepal_Length) as msl")
  # moving window aggregation
  set.seed(1)
  a = matrix(rnorm(20), nrow=5)
  A = as.scidb(a)
  b = aggregate(A, FUN=sum, window=c(0, 1, 0, 0))[]
  X = matrix(0, 5, 4)
  X[as.matrix(b[, 1:2] + 1)] = b[,3]
  check(X, apply(a, 2, function(x) x + c(x[-1], 0)))
  B = subset(A, val > 0)
  aggregate(B, FUN=sum, window=c(0, 1, 0, 0))
  aggregate(B, by="i", FUN=sum, variable_window=c(0, 1))


# Tests for issue #85 and issue #86
  tryCatch(iquery("create_array(zzz_,<acc_x:uint8,acc_y:uint8,acc_z:uint8,sleep:uint8> [subject=0:*,1,0,day=0:*,1,0,mil=0:86399999,86400000,600000],0)"), error=invisible)
  x = scidb("zzz_")
  s = 2
  sub = 2
  d = 4
  t0 = 9240001
  t1 = 45240000
  check((subset(x, subject == sub && day == d && mil>=t0 && mil<=t1)@name), (subset(x, subject == s && day == d && mil>=t0 && mil<=t1)@name))
  tryCatch(iquery("create_array(zzz_1, <population:string NOT NULL> [sample_id=0:*,200000,0,population_id=0:4,5,0],0)"), error=invisible)
  y = scidb("zzz_1")
  subset(y, population=="SAS")@name
  scidbrm(c("zzz_", "zzz_1"), force=TRUE)

# issue 88
  rm(list=c("i"))
  x = scidb("build(<val:double> [i=1:1,1,0, n=1:1,1,0], random())")
  check(nchar((subset(x, i < 5))@name), nchar((subset(x, n < 5))@name))

# issue 98
  x = scidb("build(<val:double>[i=0:3,4,0], double(i)/100)")
  check(length(subset(x, val > 0.01)[,drop=TRUE]), 2)
  y = 0.01
  check(length(subset(x, val > y)[,drop=TRUE]), 2)
}

