## Enable this line to log every significant API call made from the application
## to SciDBR.
options(scidb.trace.api="internal")

## Enable this line to log every HTTP interaction between SciDBR and
## the SciDB server.
options(scidb.trace.http=TRUE)

## Enable this line to mask out IDs and hashes in the log
## to make it easier to compare logs across multiple runs.
options(scidb.log.mask=TRUE)


## After an error, display a stack trace and stop logging.
## This makes it less confusing to find where the error happened in the log.
options(error=function() {
  traceback(3)
  options(error=NULL,
          scidb.trace.api=NULL,
          scidb.trace.http=NULL)
  stop("Test a.R exited with error")
})

library("Matrix")
library("scidb")

test_with_security = (Sys.getenv("SCIDB_TEST_WITH_SECURITY", "") == "true")
is_arrow_installed = 'arrow' %in% .packages(all.available=TRUE)

`%||%` = function(a, b) { if (length(a) > 0) a else b }

check = function(a, b) {
  print(match.call())
  stopifnot(all.equal(a, b, check.attributes=FALSE, check.names=FALSE))
}

connect = function(secure=NULL, port=NULL, ...) {
  host = Sys.getenv("SCIDB_TEST_HOST")
  if (is.null(host) || nchar(host) == 0) {
    stop("No SciDB host found. Please set $SCIDB_TEST_HOST.")
  }
  
  if (is.null(secure)) {
    secure = test_with_security
  }
  
  if (!secure) {
    db = scidbconnect(host, 
                      port=port %||% Sys.getenv("SCIDB_TEST_PORT", 8080),
                      ...)
  } else {
    db = scidbconnect(host,
                      port=port %||% Sys.getenv("SCIDB_TEST_PORT", 8083),
                      username="root", 
                      password="Paradigm4", 
                      protocol="https", 
                      ...)
  }
  return(db)
}

#' Reconnect to the same SciDB server, overriding old settings with the ones
#' specified in the optional argument list.
reconnect = function(oldconn, ...) {
  if (inherits(oldconn, "afl")) {
    oldconn = attr(oldconn, "connection")
  }
  args = list(...)
  username = args$username %||% oldconn$username
  password = args$password %||% oldconn$password
  if (is.null(password) && !is.null(username) && only(username) == "root") {
    ## The password gets erased after connecting, so we need to resupply it.
    password = "Paradigm4"
  }
  newdb = scidbconnect(
    host=args$host %||% oldconn$host,
    port=args$port %||% oldconn$port,
    protocol=args$protocol %||% oldconn$protocol,
    username=username,
    password=password,
    ...
  )
  return(newdb)
}

## Run tests in a function, so local variables get bound to function scope
## and will be gc-able when the function ends.
run_tests = function(db) {
  conn_type = class(db)[[1]]
  message("\n\n===== Starting tests on ", conn_type, " connection =====")
  t1 = proc.time()

  message("--- 1. Data movement tests ---")

  message("1.1. upload data frame")
  x = as.scidb(db, iris)
  a =  schema(x, "attributes")$name

  message("1.2. binary download")
  check(iris[, 1:4], as.R(x)[, a][, 1:4])

  message("1.3. iquery binary download")
  check(iris[, 1:4], iquery(db, x, return=TRUE)[, a][, 1:4])

  message("1.4. iquery CSV download")
  check(iris[, 1:4], iquery(db, x, return=TRUE, binary=FALSE)[, a][, 1:4])

  message("1.5. iquery Arrow download")
  if (is_arrow_installed) {
    check(iris[, 1:4], iquery(db, x, return=TRUE, 
                              arrow=TRUE, binary = FALSE)[, a][, 1:4])
  } else {
    warning("Arrow test was skipped because arrow library isn't installed")
  }

  message("1.6. as.R only attributes")
  check(iris[, 1],  as.R(x, only_attributes=TRUE)[, 1])

  message("1.7. only_attributes")
  check(as.R(db$op_count(x))$count, nrow(as.R(x)))
  check(as.R(db$op_count(x))$count, nrow(as.R(x, only_attributes=TRUE)))

  message("1.8. supply full schema to skip metadata query")
  a = scidb(db, x@name, schema=schema(x))
  check(as.R(db$op_count(x))$count, nrow(as.R(a)))

  message("1.9. supply abbreviated schema to skip metadata query")
  a = scidb(db, x@name, schema=gsub("\\[.*", "", schema(x)))
  check(as.R(db$op_count(x))$count, nrow(as.R(a)))

  message("1.10. upload using aio_input")
  ## download using only_attributes (because aio_input adds dimensions
  ##  that we don't care about)
  x = as.scidb(db, iris, use_aio_input=TRUE)
  check(iris[, 1:4], as.R(x, only_attributes=TRUE)[, 1:4])

  message("1.11. upload vector")
  check(1:5, as.R(as.scidb(db, 1:5))[, 2])
  
  message("1.12. upload matrix")
  x = matrix(rnorm(100), 10)
  check(x, matrix(as.R(as.scidb(db, x))[, 3], 10, byrow=TRUE))
  
  message("1.13. upload csparse matrix, also check shorthand projection syntax")
  x = Matrix::sparseMatrix(i=sample(10, 10), j=sample(10, 10), x=runif(10))
  y = as.R(as.scidb(db, x))
  check(x, Matrix::sparseMatrix(i=y$i + 1, j=y$j + 1, x=y$val))
  
  message("1.14. issue #126")
  df = as.data.frame(matrix(runif(10*100), 10, 100))
  sdf = as.scidb(db, df)
  check(df, as.R(sdf, only_attributes=TRUE))
  
  message("1.15. issue #130")
  df = data.frame(x1 = c("NA", NA), x2 = c(0.13, NA), x3 = c(TRUE, NA), stringsAsFactors=FALSE)
  x = as.scidb(db, df)
  check(df, as.R(x, only_attributes=TRUE))

  message("1.16. upload n-dimensional array")
  brick = 1:60
  dim(brick) = c(5, 4, 3)
  x = as.scidb(db, brick)
  ## Turn the brick into a dataframe like this, to compare against scidb:
  ##    i1 i2 i3 val
  ## 1   0  0  0   1  (the value at brick[[1, 1, 1]])
  ## 2   0  0  1  21  (brick[[1, 1, 2]])
  ## 3   0  0  2  41  (brick[[1, 1, 3]])
  ## 4   0  1  0   6  (brick[[1, 2, 1]])
  ## 5   0  1  1  26  ...
  ## 6   0  1  2  46
  ## ...
  ## 58  4  3  0  20
  ## 59  4  3  1  40
  ## 60  4  3  2  60
  brick_df = data.frame()
  for (i1 in 1:dim(brick)[[1]]) {
    for (i2 in 1:dim(brick)[[2]]) {
      for (i3 in 1:dim(brick)[[3]]) {
        brick_df <- rbind(brick_df, 
                          list(i1=i1 - 1, 
                               i2=i2 - 1, 
                               i3=i3 - 1,
                               val=brick[[i1, i2, i3]]))
      }
    }
  }
  check(brick_df, as.R(x))

  ## Trigger garbage collection. Note this won't remove any arrays
  ## because the local variables are still in scope - to really remove
  ## the arrays, we need to gc() after this function exits.
  gc()


 message("\n--- 2. AFL tests ---")

 message("2.1. Issue #128")
 i = 4
 j = 6
 x = db$build("<v:double>[i=1:2,2,0, j=1:3,1,0]", i * j)
 check(sort(as.R(x)$v), c(1, 2, 2, 3, 4, 6))
 x = db$apply(x, w, R(i) * R(j))
 # Need as.integer() for integer64 coversion below
 check(as.integer(as.R(x)$w), rep(24, 6))


 message("\n--- 3. Miscellaneous tests ---")

 message("3.1. issue #156 type checks")

 message("3.1.1 reconnect with int64=TRUE")
 db = reconnect(db, int64=TRUE)
 x = db$build("<v:int64>[i=1:2,2,0]", i)
 check(as.R(x), as.R(as.scidb(db, as.R(x, TRUE))))

 message("3.1.2 reconnect with int64=FALSE")
 db = reconnect(db, int64=FALSE)
 x = db$build("<v:int64>[i=1:2,2,0]", i)
 check(as.R(x), as.R(as.scidb(db, as.R(x, TRUE))))

 message("3.2. Issue #157")
 x = as.R(scidb(db, "build(<v:float>[i=1:5], sin(i))"), binary = FALSE)

 message("3.3. Issue #163")
 x = as.scidb(db, serialize(1:5, NULL))
 y = as.R(x)
 check(y$val[[1]], serialize(1:5,NULL))

 iquery(db, "build(<val:binary>[i=1:2,10,0], null)", return=TRUE)

  message("3.4. Test for issue #161")
  iquery(db, "op_count(list())", return=TRUE, only_attributes=TRUE,  binary=FALSE)

  message("3.5. Test for issue #158")
  x = iquery(db, "join(op_count(build(<val:int32>[i=0:234,100,0],random())),op_count(build(<val:int32>[i=0:1234,100,0],random())))", 
        schema = "<apples:uint64, oranges:uint64>[i=0:1,1,0]", return=TRUE)
  check(names(x), c("i", "apples", "oranges"))

  message("3.6. issue #160 deal with partial schema string")
  x = iquery(db, "project(list(), name)", schema="<name:string>[No]", return=TRUE)
  check(names(x), c("No", "name"))
  iquery(db, "build(<val:double>[i=1:3;j=1:3], random())", return=T, schema="<val:double>[i; j]")
  iquery(db, "build(<val:double>[i=1:3;j=1:3], random())", return=T, schema="<val:double>[i=1:3:0:3;j=1:3:0:3]")
  iquery(db, "build(<val:double>[i=1:3;j=1:3], random())", return=T, schema="<val:double>[i=1:3,1,0,j=1:3,1,0]")
  iquery(db, "build(<val:double>[i=1:3;j=1:3], random())", return=T, schema="<val:double>[i=1:3,1,0;j=1:3,1,0]")
  iquery(db, "build(<val:double>[i=1:3;j=1:3], random())", return=T, schema="<val:double>[i=1:3;j=1:3]")
  iquery(db, "build(<val:double>[i=1:3;j=1:3], random())", return=T, schema="<val:double>[i,j]")

  message("3.7. basic types from scalars")
  lapply(list(TRUE, "x", 420L, pi), function(x) check(x, as.R(as.scidb(db, x))$val))

  message("3.8. trickier types")
  x = Sys.Date()
  check(as.POSIXct(x, tz="UTC"), as.R(as.scidb(db, x))$val)
  x = iris$Species
  check(as.character(x), as.R(as.scidb(db, x))$val)

  message("3.9. type conversion from data frames")
  x = data.frame(a=420L, b=pi, c=TRUE, d=factor("yellow"), e="SciDB", f=as.POSIXct(Sys.Date(), tz="UTC"), stringsAsFactors=FALSE)

  message("3.10. issue #164 improper default value parsing")
  tryCatch(iquery (db, "remove(x)"), error=invisible)
  iquery(db, "create array x <x:double not null default 1>[i=1:10]")
  as.R(scidb(db, "x"))
  tryCatch(iquery (db, "remove(x)"), error=invisible)

  message("3.11. issue #158 support empty dimension spec []")
  iquery(db, "apply(build(<val:double>[i=1:3], random()), x, 'abc')", return=TRUE,
         schema="<val:double,  x:string>[]", only_attributes=TRUE)

  message("3.12. issue #172 (uint16 not supported)")
  iquery(db, "list('instances')", return=TRUE, binary=TRUE)

  message("3.13. Test for references and garbage collection in AFL statements")
  x = store(db, db$build("<x:double>[i=1:1,1,0]", R(pi)))
  y = db$apply(x, "y", 2)
  rm(x)
  gc()
  as.R(y)
  rm(y)

  message("3.14. Issue 191 scoping issue example")
  a = db$build("<val:double>[x=1:10]", 'random()')
  b = db$aggregate(a, "sum(val)")
  as.R(b)
  foo = function() {
     c = db$build("<val:double>[x=1:10]", 'random()')
     d = db$aggregate(c, "sum(val)")
     as.R(d)
  }
  foo()

  message("3.15 Issue 193 Extreme numeric values get truncated on upload")
  upload_data <- data.frame(a = 1.23456e-50)
  upload_ref <- as.scidb(db, upload_data)
  download_data <- as.R(upload_ref, only_attributes = TRUE)
  stopifnot(upload_data$a == download_data$a)

  message("3.16 Issue 195 Empty data.frame(s)")
  for (scidb_type in names(scidb:::.scidbtypes)) {
    for (only_attributes in c(FALSE, TRUE)) {
      cat("Testing empty data frame of type ", scidb_type, 
          " with only_attributes=", only_attributes, sep="", fill=TRUE)
      one_df <- iquery(
        db,
        paste("build(<x:", scidb_type, ">[i=0:0], null)"),
        only_attributes = only_attributes,
        return = TRUE)
      empty_df <- iquery(
        db,
        paste("filter(build(<x:", scidb_type, ">[i=0:0], null), false)"),
        only_attributes = only_attributes,
        return = TRUE)
      index <- 1 + ifelse(only_attributes, 0, 1)
      if (class(one_df) == "data.frame") {
        stopifnot(class(one_df[, index]) == class(empty_df[, index]))
        merge(one_df, empty_df)
      }
      else {
        stopifnot(class(one_df[[index]]) == class(empty_df[[index]]))
        mapply(c, one_df, empty_df)
      }
    }
  }

  message("3.17 Issue 195 Coerce very small floating point values to 0")
  small_df <- data.frame(a = .Machine$double.xmin,
                         b = .Machine$double.xmin / 10,   # Will be coerced to 0
                         c = -.Machine$double.xmin,
                         d = -.Machine$double.xmin / 10)  # Will be coerced to 0
  small_df_db <- as.R(as.scidb(db, small_df), only_attributes = TRUE)
  small_df_fix <- small_df
  small_df_fix$b <- 0
  small_df_fix$d <- 0
  print(small_df_fix)
  print(small_df_db)
  check(small_df_db, small_df_fix)
  
  message("3.18. Issue 217 Upload vectors, matrices as temp arrays via as.scidb")
  iris_mod = iris
  colnames(iris_mod) = gsub(pattern = '[.]', replacement = '_', x = colnames(iris_mod))
  DF = as.scidb(db, iris_mod, temp = T)
  VEC = as.scidb(db, 1:10, temp = T)
  MAT = as.scidb(db, as.matrix(iris_mod[1:3]), temp = T)
  dgc_mat = Matrix(c(0, 0,  0, 2,
                     6, 0, -1, 5,
                     0, 4,  3, 0,
                     0, 0,  5, 0),
                   byrow = TRUE, nrow = 4, sparse = TRUE)
  rownames(dgc_mat) = paste0('r', 1:4)
  colnames(dgc_mat) = paste0('c', 1:4)
  DGCMAT = as.scidb(db, dgc_mat, temp = T)
  check(all(c(DF@name, VEC@name, MAT@name, DGCMAT@name) %in% 
              iquery(db, "filter(list(), temporary=TRUE)", return = T)$name),
        TRUE)
  
  message("3.19. Issue 220 Upload long vectors via as.scidb")
  if (test_with_security) { # The following tests for long vector upload do not run OK on Docker SciDB setups 
                            # that have low system RAM. 
                            # Disabling the tests for SciDB CE Docker setup (that is used on 
                            # Github Actions infrastructure)
    check_long_vector_upload_as.scidb <- function(db, data, verbose = FALSE, max_byte_size) {
      if(verbose) message('Vector length: ', length(data))
      if(verbose) message('Object size: ', format(object.size(data), units = 'Mb'))
      
      if(verbose) message('Loading vector to SciDB...')
      if(missing(max_byte_size)) {
        data_scidb = as.scidb(db, data)
      } else {
        data_scidb = as.scidb(db, data, max_byte_size=max_byte_size)
      }
      data_name = data_scidb@name
      if(verbose) message('Loaded to SciDB. Object name: ', data_name)
      
      if(verbose) message('Retrieving from SciDB...')
      data_r = as.R(data_scidb)
      if(verbose) message('Retrieved object size: ', format(object.size(data_r), units = 'Mb'))
      data_r = data_r[order(data_r$i),]
      
      if(verbose) message('Testing uploaded vector with provided vector for equality - ')
      check(data, data_r$val)
      if(verbose)  message('Uploaded vector is equal to provided vector.') 
      rm(data, data_scidb, data_r); gc()
      
      if(verbose) message('Deleting from SciDB...')
      if(data_name %in% iquery(db, 'list()', return = TRUE)$name) iquery(db, paste0('remove(', data_name, ')'))
      if(verbose) message('Vector deleted from SciDB.')
    }
    
    # Recording global options to revert back to original values after executing the tests
    initial.max_byte_size = getOption('scidb.max_byte_size')
    initial.result_size_limit = getOption('scidb.result_size_limit')
    
    # Setting 'scidb.max_byte_size' to 40Mb as this will allow testing multi-part uploading of long vectors via
    # as.scidb() on reasonably sized vectors and not cause problems with R memory allocation.
    options(scidb.max_byte_size = 40*(10^6))
    options(scidb.result_size_limit = 1000)
    # integer - block size is  4*(10^7)/8 = 5*(10^6)
    check_long_vector_upload_as.scidb(db, data = sample(x=1:10, size = 10^7, replace=TRUE), verbose=F)
    # float - block size (4*(10^7))/8=5*(10^6)
    check_long_vector_upload_as.scidb(db, data = sample(x=c(1:100/10), size = 10^7, replace=TRUE), verbose=F)
    # float - with a specified max_byte_size
    check_long_vector_upload_as.scidb(db, data = sample(x=c(1:100/10), size = 10^7, replace=TRUE), verbose=F,
                                      max_byte_size = 10*10^6)
    # character - block size (4*(10^7))/2=2*10^7
    check_long_vector_upload_as.scidb(db, data = sample(x=letters, size = 10^7.8, replace=TRUE), verbose=F)
    
    # Restoring global options
    options(scidb.max_byte_size = initial.max_byte_size)
    options(scidb.result_size_limit = initial.result_size_limit)
  } else {
    warning("Security test was skipped because SCIDB_TEST_WITH_SECURITY is not true")
  }
 
  message("3.20. Issue 224 Support for SciDB dataframe")
  scidb_df_name = 'scidb_df_flat_test'
  scidb_df = data.frame(i=c(rep(1,3), rep(2,3), rep(3,3)), j=rep(1:3, 3), stringsAsFactors = FALSE)
  scidb_df$value = as.numeric(scidb_df$i == scidb_df$j)
  message("3.20.1. create a SciDB dataframe")
  iquery(db, 
         sprintf("store(flatten(build(<value:double>[i=1:3:0:1, j=1:3:0:1], iif(i=j, 1, 0))), %s)", 
                 scidb_df_name)
         )
  message("3.20.2. check iquery")
  scidb_ret <- iquery(db, sprintf('scan(%s)', scidb_df_name), return = TRUE)
  scidb_ret <- scidb_ret[order(scidb_ret$i, scidb_ret$j),]
  check(scidb_df, scidb_ret)
  message("3.20.3. check as.R")
  scidb_ret <- as.R(scidb(db, scidb_df_name))
  scidb_ret <- scidb_ret[order(scidb_ret$i, scidb_ret$j),]
  check(scidb_df, scidb_ret)
  message("3.20.4. Delete SciDB dataframe")
  iquery(db, sprintf('remove(%s)', scidb_df_name))
  message("3.20.5. Check an SciDB dataframe created from an array with a single dimension")
  scidb_ret <- iquery(db, 'flatten(build(<value:double>[i=1:5:0:1], iif(i%2=0, 1, 0)))', return=T)
  scidb_ret <- scidb_ret[order(scidb_ret$i),]
  check(scidb_ret, data.frame(i = 1:5, value = abs(floor(1:5%%2) -1), stringsAsFactors = FALSE))

  message("\n")
  message("Ran tests on ", conn_type, " connection",
          " in ", (proc.time()-t1)[[3]], " seconds")
  message("===== Finished tests on ", conn_type, " connection =====")
}

## Run tests on the given connection, then perform garbage collection
## and check for arrays created by the test that weren't cleaned up.
run_tests_with_gc = function(db) {
  preexisting <- ls(db)$name
  message("Pre-existing SciDB arrays: ", 
          if (length(preexisting) == 0) "none"
          else paste(preexisting, collapse=", "))
  
  run_tests(db)
  
  ## Garbage collection is only effective after the variables in run_tests()
  ## have gone out of scope.
  gc()
  
  remaining <- ls(db)$name
  message("Remaining SciDB arrays after test: ", 
          if (length(remaining) == 0) "none"
          else paste(remaining, collapse=", "))
  
  unexpected <- setdiff(remaining, preexisting)
  if (length(unexpected) > 0) {
    stop("These arrays were created during the test and were not cleaned up: ",
         paste(unexpected, collapse=", "))
  }
}

http_port = Sys.getenv("SCIDB_HTTPAPI_PORT", 
                       Sys.getenv("SCIDB_TEST_PORT", 8239))
message("Attempting to connect to httpapi on port ", http_port)
start = proc.time()[["elapsed"]]
httpdb = tryCatch(connect(port=http_port),
                   error=function(e) {
                     message("Could not connect to httpapi: ", 
                             conditionMessage(e))
                   })
if (!is.null(httpdb) && !inherits(httpdb, "httpapi")) {
  message("Connection on port ", attr(httpdb, "connection")$port,
          " is not an httpapi connection; skipping httpapi tests.")
  httpdb = NULL
}
if (!is.null(httpdb)) {
  message("Connected to httpapi in ", proc.time()[["elapsed"]]-start,
          " seconds")
  run_tests_with_gc(httpdb)
}

message("\n\nAttempting to connect to shim")
start <- proc.time()[["elapsed"]]
shimdb <- tryCatch(connect(), 
                   error=function(e) {
                     message("Could not connect to Shim: ", 
                             conditionMessage(e))
                   })
if (!is.null(shimdb) && !inherits(shimdb, "shim")) {
  message("Connection on port ", attr(httpdb, "connection")$port,
          " is not a shim connection; skipping shim tests.")
  shimdb = NULL
}
if (!is.null(shimdb)) {
  message("Connected to shim in ", proc.time()[["elapsed"]]-start, " seconds")
  run_tests_with_gc(shimdb)
}

if (!is.null(shimdb)) {
  message("Successfully tested shim connection on port ", 
          attr(shimdb, "connection")$port)
}
if (!is.null(httpdb)) {
  message("Successfully tested httpapi connection on port ", 
          attr(httpdb, "connection")$port)
}
if (is.null(httpdb) && is.null(shimdb)) {
  stop("Didn't connect to shim or httpapi; tests were not run.")
}
