\name{svd}
\alias{svd}
\title{
svd
}
\description{
Compute the singular value decomposition of a dense SciDB matrix.
}
\usage{
svd(x, nu = min(n, p), nv = min(n, p), LINPACK = FALSE)
}
\arguments{
  \item{x}{A scidb object representing a dense matrix.}
  \item{nu}{Optional number of singular values/vectors to compute. See notes below.}
  \item{nv}{Not used.}
  \item{LINPACK}{Not used.}
}
\note{
If the \code{nu} argument is specified, the SciDB \code{tsvd} function will be
used. Otherwise the full SVD is computed by SciDB. The \code{nv} and
\code{LINPACK} arguments are not used and appear in the function signature only
for compatibility with the base \code{svd} function.
}
\value{
A list of three \code{scidb} objects corresponding to a matrix of left
singular vectors, a vector of singular values, and a matrix of right
singular vectors, respectively.
}
\author{
B. W. Lewis<blewis@paradigm4.com>
}
\seealso{
  \code{\link{scidb}}
}
\examples{
\dontrun{
x <- as.scidb(matrix(rnorm(500*500),500))
y <- svd(x)
}}
