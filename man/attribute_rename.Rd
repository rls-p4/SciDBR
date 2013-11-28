\name{attribute_rename}
\alias{attribute_rename}
\title{
attribute_rename
}
\description{
The \code{attribute_rename} function is a wrapper to the SciDB
`attribute_rename` operator.
}
\usage{
attribute_rename(x, old, `new`, `eval`=FALSE)
}
\arguments{
  \item{x}{A scidb or scidbdf object}
  \item{old}{Old attribute name (character)}
  \item{new}{New attribute name (character)}
  \item{eval}{(Optional) If true, execute the query and store the reult array. Otherwise defer evaluation. By default, \code{eval} is set to defer evaluation only when called from functions in the \pkg{scidb} package.}
}
\value{
A \code{scidbdf} object.
}
\note{
Rename a single attribute at a time.
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
