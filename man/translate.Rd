\name{translate}
\alias{translate}
\title{
translate
}
\description{
Translate the starting coordinates of a SciDB array.
}
\usage{
translate(x, origin="origin", newstart, newchunk)
}
\arguments{
  \item{x}{A SciDB array object of class scidb or scidbdf.}
  \item{origin}{Either the string "origin" indicating zero, or a vector of new starting indices for each dimension of the array \code{x}}
  \item{newstart}{Optional vector of new array starting coordinates. Defaults to origin.}
  \item{newchunk}{Optional vector of new array chunk sizes.}
}
\value{
A \code{scidb} object with translated origin.
}
\note{
Unless \code{origin="origin"}, an array with unbounded upper-coordinate bounds
is returned. Used in combination with the \code{bound} function to return a
bounded array in that case.
}
\author{
B. W. Lewis<blewis@paradigm4.com>
}
\seealso{
  \code{\link{bound}, \link{subarray}}
}
\examples{
\dontrun{
# Add chunk overlap to an array:

x <- as.scidb(matrix(rnorm(25),5), start=c(5,15))
schema(x)
#[1] "<val:double NULL DEFAULT null> [i=5:9,5,0,j=15:19,5,0]"

y <- translate(x)
schema(y)
#[1] "<val:double NULL DEFAULT null> [i=0:4,5,0,j=0:4,5,0]"

z <- translate(x,c(0,0))
#[1] "<val:double NULL DEFAULT null> [i=0:*,5,0,j=0:*,5,0]"
}}
