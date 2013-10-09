\name{svd}
\alias{svd}
\title{
svd
}
\description{
Compute the singular value decomposition of a dense SciDB matrix.
}
\usage{
svd(x)
}
\arguments{
  \item{x}{A scidb object representing a dense matrix.}
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
