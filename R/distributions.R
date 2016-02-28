# Probability distribution and quantile functions
# These functions require the Paradigm4 extensions to SciDB.

setOldClass("phyper")
setGeneric("phyper", function(x, ...) stats::phyper(x,...))

setOldClass("qhyper")
setGeneric("qhyper", function(x, ...) stats::qhyper(x,...))

#' The Hypergeometric distribution
#'
#' Density and quantile function for the hypergeometric distribution.
#' @param  x vector of quantiles representing the number of white balls
#'           drawn without replacement from an urn which contains both
#'           black and white balls.
#' @param  q vector of quantiles representing the number of white balls
#'           drawn without replacement from an urn which contains both
#'           black and white balls.
#' @param  m the number of white balls in the urn.
#' @param  n the number of black balls in the urn.
#' @param  k the number of balls drawn from the urn.
#' @param new the name of the new attribute with the resulting probabilities
#' @return a new \code{scidb} array object
#' @seealso \code{\link{phyper}} \code{\link{qhyper}}
#' @export
setMethod("phyper", signature(x="scidb"),
  function(x, q, m, n, k, new="p")
  {
    query = sprintf("hygecdf(%s,%s,%s,%s)",q,m,n,k)
    bind(x, new, query)
  })

#' The Hypergeometric distribution
#'
#' Density and quantile function for the hypergeometric distribution.
#' @param  x vector of quantiles representing the number of white balls
#'           drawn without replacement from an urn which contains both
#'           black and white balls.
#' @param  p vector of quantiles representing the number of white balls
#'           drawn without replacement from an urn which contains both
#'           black and white balls.
#' @param  m the number of white balls in the urn.
#' @param  n the number of black balls in the urn.
#' @param  k the number of balls drawn from the urn.
#' @param new the name of the new attribute with the resulting quantiles
#' @return a new \code{scidb} array object
#' @seealso \code{\link{phyper}} \code{\link{qhyper}}
#' @export
setMethod("qhyper", signature(x="scidb"),
  function(x, p, m, n, k, new="q")
  {
    query = sprintf("ihygecdf(%s,%s,%s,%s)",p,m,n,k)
    bind(x, new, query)
  })

#' Fisher's Exact Test for Count Data
#'
#' Performs Fisher's exact test for testing the null of independence
#' of rows and columns in a contingency table with fixed marginals.
#' For 2 by 2 tables, the null of conditional independence is equivalent to the
#' hypothesis that the odds ratio equals one. "Exact" inference can be based on
#' observing that in general, given all marginal totals fixed, the first element
#' of the contingency table has a non-central hypergeometric distribution with
#' non-centrality parameter given by the odds ratio (Fisher, 1935).
#' 
#' Consider the following 2x2 contingency table:
#' \tabular{lccc}{
#' \tab Class I YES\tab Class I NO\tab SUM\cr
#' Class II YES\tab x\tab a\tab k = x + a\cr
#' Class II NO\tab b\tab c\tab \cr
#' SUM\tab m = x + b\tab n = a + c\tab \cr
#' }
#' The \code{x} input value specifies the name of the SciDB array attribute that
#' indicates the number of 'yes' events in both classifications.
#' The \code{m} input value specifies the name of the SciDB array attribute that
#' indicates the marginal sum of the first column.
#' The \code{n} input value specifies the name of the SciDB array attribute that
#' indicates the marginal sum of the second column.
#' The \code{k} input value specifies the name of the SciDB array attribute that
#' indicates the marginal sum of the first row.
#' @param a SciDB array
#' @param x yes count attribute name
#' @param m marginal count attribute name
#' @param n marginal count attribute name
#' @param k amrginal count attribute name
#' @param alternative one of "two.sided", "greater", "less"
#' @return A new SciDB array with two new attributes (note that the returned attribute
#' names may be adjusted to account for naming conflicts with existing array attributes):
#' pvalue (the p-value of the test), and estimate (an estimate of the odds ratio).
#' Note that the conditional maximum likelihood estimate (MLE) rather than the
#' unconditional MLE (the sample odds ratio) is used.
#' @export
#' @seealso \code{\link{scidb}} \code{\link{phyper}} \code{\link{qhyper}} \code{\link{dhyper}}
#' @examples
#' \dontrun{
#' # Create a test array:
#' a <- scidb("apply(build(<x:int64>[i=0:0,1,0],2),m,12,n,18,k,17)")
#' scidb_fisher.test(a)[]
#' 
#' # output looks like:
#' #   x  m  n  k         pval   estimate
#' # 0 2 12 18 17 0.0005367241 0.04693664
#' }
scidb_fisher.test = function(a, x="x", m="m", n="n", k="k", alternative=c("two.sided", "greater", "less"))
{
  alternative = match.arg(alternative)
  pvalname = make.unique_(scidb_attributes(a), "pval")
  oddsname = make.unique_(scidb_attributes(a), "estimate")
  query = sprintf("apply(%s, %s, fishertest_p_value(%s,%s,%s,%s,'%s'), %s, fishertest_odds_ratio(%s,%s,%s,%s))",
           a@name, pvalname, x, m, n, k, alternative, oddsname, x, m, n, k)
  .scidbeval(query, depend=list(a), gc=TRUE)
}
