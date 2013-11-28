\name{cast}
\alias{cast}
\title{
cast
}
\description{
The \code{cast} function is a wrapper to the SciDB `cast` operator.

}
\usage{
cast(x, s, `eval`=FALSE)
}
\arguments{
  \item{x}{A scidb or scidbdf object}
  \item{s}{An optional scidb schema character string}
  \item{eval}{(Optional) If true, execute the query and store the reult array. Otherwise defer evaluation. By default, \code{eval} is set to defer evaluation only when called from functions in the \pkg{scidb} package.}
}
\value{
A \code{scidbdf} object.
}
\note{
If the \code{s} schema string argument is missing, the cast function
strips the "not nullable" property from all attributes in the array \code{x}.
Use \code{eval=FALSE} to defer expression evaluation for efficiency.
}
\author{
B. W. Lewis<blewis@paradigm4.com>
}
\seealso{
  \code{\link{scidb}}
}
\examples{
\dontrun{
# XXX Write me
}}
