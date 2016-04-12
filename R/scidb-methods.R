setGeneric("c")
setGeneric("head")
setGeneric("is.scidb", function(x) standardGeneric("is.scidb"))

#' Concatenate two SciDB arrays
#'
#' Concatenate two SciDB arrays with common schema along their last dimension.
#' @param x \code{scidb} array object
#' @param y \code{scidb} array object
#' @return \code{scidb} array object
#' @importFrom methods setMethod setGeneric
#' @export
setMethod(c, signature(x="scidb"),
function(x, y)
{
  if(as.numeric(scidb_coordinate_bounds(x)$length) < as.numeric(.scidb_DIM_MAX))
  {
    s = sprintf("%s%s",build_attr_schema(x), build_dim_schema(x, newend=.scidb_DIM_MAX))
    x = redimension(x,s)
  }
  i = count(x) + as.numeric(scidb_coordinate_start(x)) - as.numeric(scidb_coordinate_start(y))
  j = make.unique_(scidb_attributes(y), "j")
  fun = sprintf("%s + %.0f", dimensions(y), i)
  s = sprintf("apply(%s, %s, %s)",y@name, j, fun)
  scma = sprintf("%s%s",build_attr_schema(y), build_dim_schema(x, newnames=j))
  s = sprintf("redimension(%s, %s)",s, scma)
  s = sprintf("cast(%s, %s%s)", s,build_attr_schema(y), build_dim_schema(x))
  s = sprintf("merge(%s, %s)", x@name, s)
  .scidbeval(s, gc=TRUE, depend=list(x, y))
})

#' Return the first part of a SciDB array
#' @param x a \code{scidb} object
#' @param n maximum number of rows to return
#' @return a data frame with the first part of the array data
#' @export
setMethod("head", signature(x="scidb"),
function(x, n=6L)
{
  iqdf(x, n)[,-1]
})

#' Test if an object has class "scidb"
#' @param x a \code{scidb} object
#' @aliases is.scidb
#' @return \code{TRUE} if \code{x} has class "scidb"
#' @export
setMethod("is.scidb", signature(x="ANY"), 
function(x)
  {
    if(inherits(x, "scidb")) return(TRUE)
    FALSE
  }
)

#' Print a summary of a \code{scidb} object
#' @param x a \code{scidb} object
#' @return printed object summary
#' @export
setMethod("print", signature(x="scidb"),
  function(x) {
    show(x)
  })

#' Histogram
#'
#' Return and optionall plot a histogram from SciDB array values
#' @param x a \code{scidb} object with a single numeric SciDB attribute
#' @param breaks a single number giving the number of cells for the histogram
#' @param right logical; if \code{TRUE}, the histogram cells are right-closed (left open) intervals
#' @param materialize logical; if \code{TRUE}, return the histogram object to R
#' @param plot logical; if \code{TRUE} plot the output
#' @param ... further arguments and graphical parameters passed to plotting routinesif \code{plot=TRUE}
#' @return an R histogram object
#' @seealso \code{\link{hist}}
#' @export
#' @importFrom graphics hist
setMethod("hist",signature(x="scidb"), hist_scidb)

#' Print a summary of a \code{scidb} object
#' @param object a \code{scidb} object
#' @return printed object summary
#' @export
setMethod("show", "scidb",
  function(object) {
    .scidbstr(object)
  })

regrid_scidb = function(x, grid, expr)
  {
    if(missing(expr)) expr = paste(sprintf("max(%s)", scidb_attributes(x)), collapse=",")
    if(is.function(expr))
    {
      expr = paste(as.character(as.list(match.call()$expr)), sprintf("(%s)", scidb_attributes(x)), collapse=",")
    }
    query = sprintf("regrid(%s, %s, %s)",
               x@name, paste(noE(grid), collapse=","), expr)
    .scidbeval(query, eval=FALSE, gc=TRUE, depend=list(x))
  }
setGeneric("regrid", function(x, grid, expr) standardGeneric("regrid"))
#' SciDB regrid decimation operator
#' @aliases regrid
#' @param x SciDB array
#' @param grid a vector of grid sizes as long as \code{length(dimensions(x))}
#' @param expr optional aggregation function applied to every attribute on the grid, or a quoted SciDB aggregation expression
#' @note The default aggregation function used in case \code{expr} is left missing is \code{max}.
#' @examples
#' \dontrun{
#' x <- as.scidb(iris)
#' regrid(x, 10, min)
#' }
#' @importFrom stats aggregate
#' @return a \code{scidb} object
#' @export
setMethod("regrid", signature(x="scidb"), regrid_scidb)

xgrid_scidb = function(x, grid)
  {
    query = sprintf("xgrid(%s, %s)", x@name, paste(noE(grid), collapse=","))
    .scidbeval(query, eval=FALSE, gc=TRUE, depend=list(x))
  }
setGeneric("xgrid", function(x, grid, expr) standardGeneric("xgrid"))
#' SciDB xgrid prolongation operator
#' @aliases xgrid
#' @param x SciDB array
#' @param grid a vector of grid sizes as long as \code{length(dimensions(x))}
#' @examples
#' \dontrun{
#' x <- as.scidb(iris)
#' y <- regrid(x, 10, min)
#' z <- xgrid(y, 10)
#' }
#' @export
setMethod("xgrid", signature(x="scidb"), xgrid_scidb)


#' Aggregate a SciDB Array Grouped by a Subset of its Dimensions and/or Attributes
#'
#' Group the \code{scidb} array object \code{x} by dimensions
#' and/or attributes in the array.  applying the valid SciDB aggregation function
#' \code{FUN} expressed as a character string to the groups. Set eval to TRUE to
#' execute the aggregation and return a scidb object; set eval to FALSE to return
#' an unevaluated SciDB array promise, which is essentially a character string
#' describing the query that can be composed with other SciDB package functions.
#' 
#' If an R reduction function is speciied for \code{FUN}, it will be
#' transliterated to a SciDB aggregate.
#' 
#' The \code{by} argument must be a list of dimension names and/or attribute names
#' in the array \code{x} to group by, or a SciDB array reference object.  If
#' \code{by} is not specified and one of the \code{window} options is not
#' specified, then a grand aggregate is performed over all values in the array.
#' 
#' The argument \code{by} may be a list of dimension names and/or attributes of the
#' array \code{x}. Attributes that are not of type int64 will be `factorized` first
#' and replaced by enumerated int64 values that indicate each unique level (this
#' requires SciDB 13.6 or higher).
#' 
#' When \code{by} is a SciDB array it must contain one or more common dimensions
#' with \code{x}.  The two arrays will be joined (using SciDB
#' \code{cross_join(x,by)} and the resulting array will be grouped by the
#' attributes in the \code{by} array. This is similar to the usual R data frame
#' aggregate method.
#' 
#' Perform moving window aggregates by specifying the optional \code{window} or
#' \code{variable_window} arguments. Use \code{window} to compute the aggregate
#' expression along a moving window specified along each coordinate axis as
#' \code{window=c(dimension_1_low, dim_1_high, dim_2_low,_dim_2_high, ...}.
#' Moving window aggregates along coordinates may be applied in multiple
#' dimensions.
#' 
#' Use \code{variable_window} to perform moving window aggregates over data
#' values in a single dimension specified by the \code{by} argument. See below
#' for examples. Moving window aggregates along data values are restricted
#' to a single array dimension.
#' @param x A \code{scidb} object.
#' @param by optional single character string or a list of array dimension and/or attribute names to group by;
#' or a \code{scidb} object to group by. Not required for \code{windowed} and grand aggregates--see details.
#' @param FUN a character string representing a SciDB aggregation expression or a reduction function.
#' @param window optional, if specified, perform a moving window aggregate along the specified coordinate windows--see details below.
#' @param variable_window optional, if specified, perform a moving window aggregate over successive data values along the
#' coordinate dimension axis specified by \code{by}--see details below.
#' @return a \code{scidb} object
#' @export
#' @examples
#' \dontrun{
#' # Create a copy of the iris data frame in a 1-d SciDB array named "iris."
#' # Note that SciDB attribute names will be changed to conform to SciDB
#' # naming convention.
#' x <- as.scidb(iris,name="iris")
#' 
#' # Compute averages of each variable grouped by Species
#' a <- aggregate(x, by="Species", FUN=mean)
#' 
#' # Aggregation by an auxillary vector (which in this example comes from
#' # an R data frame)--also note any valid SciDB aggregation expression may
#' # be used:
#' y <- as.scidb(data.frame(sample(1:4, 150, replace=TRUE)))
#' aggregate(x, by=y, FUN="avg(Petal_Width) as apw, min(Sepal_Length) as msl")[]
#' 
#' # Use the window argument to perform moving window aggregates along coordinate
#' # systems. You need to supply a window across all the array dimesions.
#' set.seed(1)
#' a <- matrix(rnorm(20), nrow=5)
#' A <- as.scidb(a)
#' # Compute a moving window aggregate only along the rows summing two rows at
#' # a time (returning result to R). The notation (0,1,0,0) means apply the
#' # aggregate over the current row (0) and (1) following row, and just over
#' # the current column (that is, a window size of one), returned in a data frame.
#' x <- aggregate(A, FUN=sum, window=c(0,1,0,0))[]
#' # Convert the returned data frame to a matrix:
#' X <- matrix(0, 5, 4)
#' X[as.matrix(x[,1:2] + 1)] <- x[,3]
#' X
#' # The above aggregate is equivalent to, for example:
#' apply(a, 2, function(x) x + c(x[-1], 0))
#' 
#' # Moving windows using the window= argument run along array coordinates.
#' # Moving windows using the variable_window= argument run along data values,
#' # skipping over empty array cells. The next example illustrates the
#' # difference.
#' 
#' # First, create an array with empty values:
#' B <- subset(A, val > 0)
#' # Here is what B looks like (in data frame form):
#' B[]
#' # Now, run a moving window aggregate along the rows with window just like
#' # the above example:
#' aggregate(B, FUN=sum, window=c(0,1,0,0))[]
#' # And now, a moving window along only the data values down the rows, note
#' # that we need to specify the dimension with by=:
#' aggregate(B, by="i", FUN=sum, variable_window=c(0,1))[]
#' }
setMethod("aggregate", signature(x="scidb"), aggregate_scidb)



setOldClass("glm")
setGeneric("glm")
#' Fitting generalized linear models
#'
#' \code{glm.fit} is used to fit generalized linear models specified by a model
#' matrix and response vector. \code{glm} is a simplified interface for
#' \code{scidb} objects similar (but much simpler than) the standard \code{glm} function.
#"
#' The \code{glm} function works similarly to a limited version of
#' the usual \code{glm} function, but with a \code{scidb} data frame-like
#' SciDB array instead of a standard data frame.
#'
#' Formulas in the \code{glm} function may only refer to variables explicitly
#' defined in the \code{data} \code{scidb} object.  That means that you should
#' bind interaction and transformed terms to your data before invoking the
#' function.  The indicated response must refer to a single-column response term
#' in the data (the two-column response form is not accepted).
#' Categorical (factor) variables in the data must be represented as strings. They
#' will be encoded as treatment-style contrast variables with the first listed
#' value set to the baseline value. No other automated contrast encodings are
#' available yet (you are free to build your own model matrix and use
#' \code{glm.fit} for that). All other variables will be coerced to
#' double-precision values.
#'
#' Use the \code{model_scidb} function to build a model matrix from a formula and
#' a \code{scidb} data frame-like SciDB array.  The matrix is returned within an
#' output list as a sparse SciDB matrix of class \code{scidb} with character
#' string variables encoded as treatment contrasts as described above.
#' If you already have a list of factor-level codes for categorical variables
#' (for example from the output of \code{glm}, you can supply that in the
#' factor argument. See help for \code{predict} for an example.
#' @rdname glm
#' @aliases glm glm.fit model_scidb
#'
#' @param x a model matrix of dimension \code{n * p}
#' @param y a response vector of length \code{n}
#' @param formula an object of class \code{formula} (or one that can be coerced to
#'          that class): a symbolic description of the model to be
#'          fitted. See details for limitations
#' @param data an object of class \code{scidb}
#' @param weights an optional vector of 'prior weights' to be used in the
#'           fitting process.  Should be \code{NULL} or a numeric or scidb vector.
#' @param family a description of the error distribution and link function to
#'          be used in the model, supplied as the result of a call to
#'          a family function
#' @param factors a list of factor encodings to use in the model matrix (see details)
#' @param intercept set to \code{TRUE} to include an intercept term in the model matrix
#' computed by \code{model_scidb}
#' @return The \code{glm.fit} and \code{glm} functions return
#' a list of model output values described below. The \code{glm}
#' functions uses an S3 class to implement printing \code{summary}, and \code{predict} methods.
#' \enumerate{
#' \item{\emph{coefficients}}{  model coefficient vector (SciDB array)}
#' \item{\emph{stderr}}{  vector of model coefficient standard errors (SciDB array)}
#' \item{\emph{tval}}{  vector of model coefficient t ratio values using estimated dispersion value (SciDB array)}
#' \item{\emph{pval}}{  vector of two-tailed p-values corresponding to the t ratio based on a Student t distribution. (It is possible that the dispersion is not known and there are no residual degrees of freedom from which to estimate it.  In that case the estimate is 'NaN'.)}
#' \item{\emph{aic}}{  a version of Akaike's \emph{An Information Criterion} value.}
#' \item{\emph{null.deviance}}{  the deviance for the null model, comparable with \code{deviance}.}
#' \item{\emph{res.deviance}}{  up to a constant, minus twice the maximized log-likelihood.}
#' \item{\emph{dispersion}}{  For binomial and Poison families the dispersion is
#'     fixed at one and the number of parameters is the number of
#'     coefficients. For gaussian, Gamma and inverse gaussian families the
#'     dispersion is estimated from the residual deviance, and the number
#'     of parameters is the number of coefficients plus one.  For a
#'     gaussian family the MLE of the dispersion is used so this is a valid
#'     value of AIC, but for Gamma and inverse gaussian families it is not. Other
#'     families set this value to \code{NA}}.
#' \item{\emph{df.null}}{  the residual degrees of freedom for the null model.}
#' \item{\emph{df.residual}}{  the residual degrees of freedom.}
#' \item{\emph{converged}}{  \code{FALSE} if the model did not converge.}
#' \item{\emph{totalObs}}{  total number of observations in the model.}
#' \item{\emph{nOK}}{  total number of observations corresponding to nonzero weights.}
#' \item{\emph{loglik}}{  converged model log-likelihood value.}
#' \item{\emph{rss}}{  residual sum of squares.}
#' \item{\emph{iter}}{  number of model iterations.}
#' \item{\emph{weights}}{  vector of weights used in the model (SciDB array).}
#' \item{\emph{family}}{  model family function.}
#' \item{\emph{y}}{  response vector (SciDB array).}
#' \item{\emph{x}}{  model matrix (SciDB array).}
#' \item{\emph{factors}}{  a list of factor variable levels (SciDB arrays)
#'                         or NULL if no factors are present in the data.}
#' }
#' @importFrom stats glm glm.fit predict
#' @export
#' @examples
#' \dontrun{
#' set.seed(1)
#' x <- as.scidb(matrix(rnorm(5000*20),nrow=5000))
#' y <- as.scidb(rnorm(5000))
#' M <- glm.fit(x, y)
#' print(M)
#'
#' counts <- c(18, 17, 15, 20, 10, 20, 25, 13, 12)
#' outcome <- gl(3, 1, 9)
#' treatment <- gl(3, 3)
#' d.AD <- data.frame(treatment, outcome, counts)
#' glm.D93 <- glm(counts ~ outcome + treatment, family = poisson(), data=d.AD, y=TRUE)
#' summary(glm.D93)
#'
#'# Compare with:
#' d.AD_sci = as.scidb(d.AD)
#' glm.D93_sci = glm(counts ~ outcome + treatment, family = poisson(), data=d.AD_sci)
#' summary(glm.D93_sci)
#' }
setMethod("glm", signature(formula="ANY", family="ANY", data="scidb"), glm_scidb)

setClassUnion("MNSN", c("missing", "NULL", "scidb", "numeric"))
setOldClass("glm.fit")
setGeneric("glm.fit")
#' @export
#' @rdname glm
setMethod("glm.fit", signature(x="scidb", y="ANY", weights="MNSN"), glm.fit_scidb)
