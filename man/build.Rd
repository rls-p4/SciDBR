\name{build}
\alias{build}
\title{
build
}
\description{
The \code{build} function is a wrapper to the SciDB `build` operator.
Operation is similar to the R \code{matrix} and \code{array} functions.
It creates a new single-attribute SciDB array based on the specified parameters.
}
\usage{
build = function(data, dim, names, type="double",
                 start, name, chunksize, overlap, gc=TRUE,
                 sparse=FALSE, condition, eval=TRUE)
}
\arguments{
  \item{data}{A valid SciDB expression (expressed as a character string) or constant to fill the array.}
  \item{dim}{A vector of dimension lengths.}
  \item{names}{An optional vector of attribute and dimension names. Default attribute name is `val` and the dimension names are labeled i,j,... by default.}
  \item{type}{The SciDB type of the array attribute.}
  \item{start}{An optional vector of starting dimension coordinate indices. Must match the length of the dim vector.}
  \item{type}{Option name of the SciDB array. An automatically generated name is used by default.}
  \item{chunksize}{An optional vector of dimension chunk sizes. Must match the length of the dim vector.}
  \item{overlap}{An optional vector of dimension overlap values. Must match the length of the dim vector.}
  \item{gc}{gc=TRUE (the default) removes the array when corresponding R objects are garbage collected. Set gc=FALSE to preserve arrays.}
  \item{sparse}{Optionally set sparse=TRUE to build a sparse array. If TRUE, the condition parameter is also required.}
  \item{condition}{A SciDB logical expression expressed as a character string that determines sparse array fill-in when TRUE.}
  \item{eval}{A logical value: if TRUE then evaluate the \code{build} expression and return a \code{scidb} object, otherwise return a \code{scidbexpr} object that contains a character string representation of the un-evaluated SciDB query. By default, eval is set to FALSE if \code{build} is called from another function in the scidb package.}
}
\value{
Either a \code{scidb} object or a \code{scidbexpr} object depending on the value of \code{eval}.
}
\note{
Use \code{eval=FALSE} to defer expression evaluation for efficiency. Many functions in the SciDB package accept un-evaluated expresions represented as \code{scidbexpr} objects. One can compose these objects to form complex query expressions that can be evaluated in one transaction.
}
\author{
B. W. Lewis<blewis@paradigm4.com>
}
\seealso{
  \code{\link{scidb}}
}
\examples{
\dontrun{
y <- build(pi, c(5,3))
print(head(y))
}}
