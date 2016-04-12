# A really basic prototype glm function, limited to simple formulas and
# treatment contrast encoding. cf glm.
glm_scidb = function(formula, family=gaussian(), `data`, `weights`)
{
  if(!is.scidb(data)) stop("data must be a scidb object")
  if(is.character(formula)) formula=as.formula(formula)
  wts = NULL
  if(!missing(weights)) wts = weights
  M = model_scidb(formula, data)
  ans = glm.fit(M$model, M$response, weights=wts, family=family,intercept=M$intercept)
  ans$formula = M$formula
  ans$coefficient_names = M$names
  ans$factors = M$factors
  ans$call = match.call()
  class(ans) = "glm_scidb"
  ans
}

# cf glm.fit
glm.fit_scidb = function(x, y, weights=NULL, family=gaussian(), intercept)
{
  got_glm = length(grep("glm",.scidbenv$ops[, 2]))>0
  if(missing(intercept)) intercept = 0
  intercept = as.numeric(intercept)
  xchunks = as.numeric(scidb_coordinate_chunksize(x))
  nrx = as.numeric(scidb_coordinate_bounds(x)$length[1])
  if(missing(`weights`)) `weights`= NULL
  if(is.numeric(`weights`))
  {
    `weights` = as.scidb(as.double(weights), chunkSize=xchunks[1])
  } else
  {
    weights = build(1.0, nrx, start=as.numeric(scidb_coordinate_start(x)[1]), chunksize=xchunks[1])
  }
  if(!is.scidb(y))
  {
    y = as.scidb(y)
  }
  if(!got_glm)
  {
    stop("The Paradigm4 glm operator was not found.")
  }
  x = replaceNA(x)
  y = replaceNA(y)
  `weights` = replaceNA(`weights`)
  dist = family$family
  link = family$link
# GLM has a some data partitioning requirements to look out for:
  ncx = as.numeric(scidb_coordinate_bounds(x)$length[2])
  if(xchunks[2] < ncx)
  {
    x = repart(x, chunk=c(xchunks[1], ncx))
  }
  xchunks = as.numeric(scidb_coordinate_chunksize(x))
  ychunks = as.numeric(scidb_coordinate_chunksize(y))
  if((ychunks[1] != xchunks[1]) )
  {
    y = repart(y, chunk = xchunks[1])
  }
  query = sprintf("glm(%s,%s,%s,'%s','%s')",
           x@name, y@name, weights@name, dist, link)
  M = .scidbeval(query, eval=TRUE, gc=TRUE)[]
  m1 = M[M[, 2] == 0, 3]
  ans = list(
    coefficients = M[M[, 1] == 0, 3],
    stderr =  M[M[, 1] == 1, 3],
    tval = M[M[, 1] == 2, 3],
    pval = M[M[, 1] == 3, 3],
    aic = m1[12],
    null.deviance = m1[13],
    res.deviance = m1[15],
    dispersion = m1[5],
    df.null = m1[6] - intercept,
    df.residual = m1[7],
    converged = m1[10] == 1,
    totalObs = m1[8],
    nOK = m1[9],
    loglik = m1[14],
    rss = m1[16],
    iter = m1[18],
    weights = weights,
    family = family,
    y = y,
    x = x
  )
# BUG HERE IN SCIDB GLM AFFECTING binomial and poisson families?
# FIXED IN SciDB 14.3
  if(compare_versions(options("scidb.version")[[1]], 14.3))
  {
    return (ans)
  }
  if(dist=="binomial" || dist=="poisson")
  {
    ans$scidb_pval = ans$pval
    ans$pval = 2 * pnorm(-abs(ans$tval[]))
  }
  ans
}

# internally used formatting utility
.format = function(x)
{
  o = options(digits=4)
  ans = paste(capture.output(x), collapse="\n")
  options(o)
  ans
}

#' Summary print method for SciDB GLM
#'
#' Print a summary of a SciDB GLM object.
#' @param x \code{glm_scidb} model object
#' @param ... other arguments to \code{summary}
#' @return Character summary of the model object is printed to standard output
#' @seealso \code{\link{summary.glm}}
#' @importFrom stats summary.glm
print.glm_scidb = function(x, ...)
{
  ans = "Call:"
  ans = paste(ans,.format(x$call),sep="\n")
  ans = paste(ans,"Formula that was used:",sep="\n\n")
  ans = paste(ans,.format(x$formula),sep="\n")
  ans = paste(ans,.format(x$family),sep="\n")

  cfs = coef(x)[]
  names(cfs) = x$coefficient_names
  ans = paste(ans,"Coefficients:",sep="\n")
  ans = paste(ans,.format(cfs),sep="\n\n")
  ans = paste(ans,sprintf("Null deviance: %.2f on %d degrees of freedom",x$null.deviance, x$df.null),sep="\n\n")
  ans = paste(ans,sprintf("Residual deviance: %.2f on %d degrees of freedom",x$res.deviance, x$df.residual),sep="\n")
  ans = paste(ans,sprintf("AIC: %.1f",x$aic),sep="\n")
  cat(ans,"\n")
}

#' Summary method for SciDB GLM
#'
#' Print a summary of a SciDB GLM object.
#' @param object \code{glm_scidb} model object
#' @param ... other arguments to \code{summary}
#' @return Character summary of the model object is printed to standard output and \code{NULL} is returned.
#' @seealso \code{\link{summary.glm}}
#' @importFrom stats summary.glm
summary.glm_scidb = function(object, ...)
{
  x = object
  ans = "Call:"
  ans = paste(ans,.format(x$call),sep="\n")
  ans = paste(ans,"Formula that was used:",sep="\n\n")
  ans = paste(ans,.format(x$formula),sep="\n")
  ans = paste(ans,.format(x$family),sep="\n")

# Coefficient table
  tbl_coef = coef(x)[]
  tbl_stderr = x$stderr[]
  tbl_zval = x$tval[]
  p = x$pval[]
  sig = c("***","**","*",".","")
  star = sig[as.integer(cut(p,breaks=c(-1,0.001,0.01,0.05,0.1,1)))]
  tbl = data.frame(tbl_coef, tbl_stderr, tbl_zval, p, star)
  colnames(tbl) = c("Estimate", "Std. Error", "z value", "Pr(>|z|)","")
  rownames(tbl) = x$coefficient_names
  ans = paste(ans,.format(tbl),sep="\n\n")
  ans = paste(ans,"---\nSignif. codes:  0 *** 0.001 ** 0.01 * 0.05 . 0.1   1",sep="\n")
  ans = paste(ans,sprintf("Dispersion parameter: %.2f",x$dispersion),sep="\n\n")
  ans = paste(ans,sprintf("Null deviance: %.2f on %d degrees of freedom",x$null.deviance, x$df.null),sep="\n\n")
  ans = paste(ans,sprintf("Residual deviance: %.2f on %d degrees of freedom",x$res.deviance, x$df.residual),sep="\n")
  ans = paste(ans,sprintf("AIC: %.1f",x$aic),sep="\n")
  ans = paste(ans,sprintf("Number of Fisher Scoring iterations: %d",x$iter),sep="\n")
  cat(ans,"\n")
}

# A limited version of a model matrix builder for linear models,
# cf model.matrix and model.frame. Returns a model matrix for the scidb
# object and the formula. String-valued variables in data are converted to
# treatment contrasts, and if present a sparse model matrix is returned.
#' @export
#' @rdname glm
model_scidb = function(formula, data, factors=NULL)
{
  if(!is.scidb(data)) stop("data must be a scidb object")
  if(is.character(formula)) formula=as.formula(formula)
  dummy = data.frame(matrix(NA, ncol=length(scidb_attributes(data))))
  names(dummy) = scidb_attributes(data)
  t = terms(formula, data=dummy)
  f = attr(t,"factors")
  v = attr(t,"term.labels")
  i = attr(t,"intercept")
  r = attr(t,"response")

  iname = c()
  if(i==1)
  {
# Add an intercept term
    iname = make.unique_(scidb_attributes(data), "intercept")
    data = bind(data, iname, "double(1)")
  }
# If the response is not present in the data, set to NA
  response_name = rownames(f)[r]
  response = NA
  if(response_name %in% scidb_attributes(data))
  {
    response = project(data,rownames(f)[r])
  }
  types = scidb_types(data)
  a = scidb_attributes(data)
  # factors (see input arguments) will contain a list of factor variables
  vars = NULL # vars will contain a list of continuous variables
# Check for unsupported formulae and warn.
  ok = v %in% scidb_attributes(data)
  if(!all(ok))
  {
    not_supported = paste(v[!ok], collapse=",")
    warning("Your formula is too complicated for this method. The following variables are not explicitly available and were not used in the model: ",not_supported)
    formula = formula(drop.terms(t, which(v %in% not_supported), keep.response=TRUE))
  }
  w = which(scidb_attributes(data) %in% c(v, iname))
  if(length(w)<1) stop("No variables to model on")
# Check to see if a list of factors was provided. If not we need to build one.
# If so, we check to make sure that the string variables in data match our
# list of provided factors (error otherwise).
  build_factors = TRUE
  if(!is.null(factors))
  {
    build_factors = FALSE
    data_factor_idx = which(types %in% "string")
    if(any(data_factor_idx) && ! all(names(factors) %in% scidb_attributes(data)[data_factor_idx]))
    {
      stop("Missing variables in input data! Please make sure your data contain all the variables in the model.")
    }
  }
  for(j in w)
  {
    if(types[j]=="string")
    {
      if(!build_factors) next
# Create a factor
      factors = c(factors, unique(project(data, j)))
      names(factors)[length(factors)] = a[j]
      next
    }
    if(types[j]!="double")
    {
# Coerce to double, preserving name
      d = make.unique_(a, sprintf("%s_double",a[j]))
      expr = sprintf("double(%s)", a[j])
      data = bind(data, d, expr)
      data = attribute_rename(data, old=c(d,a[j]),new=c(a[j],d))
    }
    vars = c(vars, a[j])
  }

  varsstr = paste(vars, collapse=",")
  query = sprintf("unfold(project(%s,%s))",data@name,varsstr)
  M = .scidbeval(query, gc=TRUE, eval=TRUE)
  M = attribute_rename(dimension_rename(M, old=c(1,2), new=c("i","j")), old=1, new="val")

  if(length(factors)<1)
  {
    return(list(formula=formula, model=M, response=response, names=vars, intercept=(i == 1), factors=factors))
  }

# Repartition to accomodate the factor contrasts, subtracting one
# if an intercept term is present (i).
  contrast_dim = 0
  for(j in factors)
  {
    contrast_dim = contrast_dim + count(j) - i
  }

  newdim = as.numeric(scidb_coordinate_bounds(M)$length[2]) + contrast_dim
  nrm = as.numeric(scidb_coordinate_bounds(M)$length[1])
  newend = c(nrm - 1, newdim - 1)
  newchunk = c(ceiling(1e6 / newdim), newdim)

  schema = sprintf("%s%s", build_attr_schema(M),
    build_dim_schema(M, newstart=c(0,0), newend=newend, newchunk=newchunk))
# Reset origin (this is a trick!)
  M = reshape_scidb(M, shape=as.numeric(scidb_coordinate_bounds(M)$length))
  M = redimension(M, schema=schema)           # Redimension to get extra columns

# Merge in the contrasts
  col = length(vars)
  varnames = vars
  for(j in 1:length(factors))
  {
    dn = make.unique_(dimensions(data), "i")
    n  = names(factors)[j]
    idx = sprintf("%s_index", n)
    idx = make.unique_(scidb_attributes(data), idx)
    y = project(index_lookup(data, factors[[j]], n, idx), idx)
    one = make.unique_(scidb_attributes(y), "val")
    column = make.unique_(scidb_attributes(y), "j")
    N = sprintf("%s%s", names(factors)[j], iquery(factors[[j]], return=TRUE)[, 2])
    if(i > 0)
    {
# Intercept term present
      y = subset(y, sprintf("%s > 0",idx))
      y = bind(y, c(one,column), c("double(1)", sprintf("int64(%s + %d - 1)", idx, col)))
      varnames = c(varnames, N[-1])
      col = col + length(N) - 1
    } else
    {
# No intercept term
      y = bind(y, c(one, column), c("double(1)", sprintf("int64(%s + %d)", idx, col)))
      varnames = c(varnames, N)
      col = col + length(N)
    }
    schema = sprintf("%s%s",
             build_attr_schema(y, newnames=c("index","val","j")),
             build_dim_schema(y, newnames="i"))
    y = redimension(reshape_scidb(cast(y, schema), shape=nrow(y)), M)
# ... merge into M
    M = merge(M, y, merge=TRUE) # eval this?
  }

  return(list(formula=formula, model=M, response=response, names=varnames, intercept=(i == 1), factors=factors))
}


# cf predict.glm
#' Predict method for SciDB GLM
#'
#' Obtains predictions and optionally estimates standard errors of
#' those predictions from a fitted generalized linear model object.
#' @param object \code{glm_scidb} model object
#' @param ... other prediction arguments including \code{newdata} (a \code{scidb} array of data to predict from),
#' \code{link} (a character value of either "link" or "response").
#' @return \code{scidb} array object
#' @seealso \code{\link{predict.glm}}
#' @importFrom stats predict.glm
#' @export
predict.glm_scidb = function(object, ...) #newdata=NULL, type=c("link","response"), se.fit=FALSE)
{
  C = match.call()
  if(is.null(C$newdata))
  {
    newdata = NULL
  } else
  {
    newdata = eval(C$newdata,envir=parent.frame())
  }
  if(is.null(C$type)) type="link"
  else type = eval(C$type,envir=parent.frame())
  if(is.null(C$se.fit)) se.fit=FALSE
  else se.fit=eval(C$se.fit,envir=parent.frame())
  if(!type %in% c("link","response")) stop("type must be one of 'link' or 'response'")
  if(is.null(newdata))
  {
    M = object$x
  } else
  {
    M = model_scidb(formula=object$formula, data=newdata, factors=object$factors)$model
  }
# This is awful...
  MM = redimension(M, sprintf("%s%s", build_attr_schema(M), build_dim_schema(M, newchunk=c(1000,1000))))
  cf = as.scidb(coef(object))
  cf = redimension(transform(cf, j=0), schema=sprintf("%s%s", build_attr_schema(cf), build_dim_schema(MM, newend=c(as.numeric(scidb_coordinate_end(MM)[2]), 0))))
  linear_predictors = scidb(sprintf("gemm(%s, %s, build(%s%s, 0))", MM@name, cf@name, build_attr_schema(MM), build_dim_schema(MM, newend=c(as.numeric(scidb_coordinate_end(MM)[1]), 0))))
  se = NULL
  if(se.fit)
  {
# Compute se.fit XXX add this
    warning("Not yet implemented")
  }
  if(type=="link") return(linear_predictors) # XXX modify to maybe return se
  pred = switch(object$family$link,
           "logit" = project(bind(linear_predictors, "fit", "exp(multiply)/(1+exp(multiply))"), "fit"),
           "identity" = linear_predictors,
           "cauchit" = stop("Not yet supported"),
           "cloglog" = stop("Not yet supported"),
           "probit" = project(bind(linear_predictors, "fit", "normcdf(multiply,0,1)"), "fit"),
           "inverse" = project(bind(linear_predictors, "fit", "1/multiply"), "fit"),
           "sqrt" = project(bind(linear_predictors, "fit", "multiply*multiply"), "fit"),
           "log" = project(bind(linear_predictors, "fit", "exp(multiply)"), "fit")
        )
  pred # XXX modify to return se
}


# This is an internally-used utility that traverses the SciDB elements of
# a glm_scidb object, applying the function f with optional arguments ...
# to each. It's used by persist.glm_scidb and others.
.traverse.glm_scidb = function(x, f, ...)
{
  f(x$coefficients, ...)
  f(x$stderr, ...)
  f(x$tval, ...)
  if(is.scidb(x$pval)) f(x$pval, ...)
  f(x$weights, ...)
  f(x$x, ...)
  f(x$y, ...)
  for(a in x$factors)
  {
    f(a, ...)
  }
}
