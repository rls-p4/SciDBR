\name{scidbremove}
\alias{scidbremove}
\alias{scidbrm}
\title{
Remove an array.
}
\description{
Remove (delete) an array from SciDB.
}
\usage{
scidbremove(x, error = warning, async, force)
}
\arguments{
  \item{x}{
The name of the SciDB array to remove (character). This
may also be a vector of names to remove.
}
  \item{error}{Error handling function.}
  \item{async}{(Optional) logical: If TRUE, use async option in newer
               versions of SciDB shim interface to return faster by not waiting
               for the query to complete. The default value is FALSE.}
  \item{force}{(Optional) logical: If TRUE, force removal of specified array,
               even if the \code{scidb.safe_remove} option is set to \code{TRUE}.}
}
\value{
NULL is invisibly returned. SciDB errors are returned as
normal R errors and may be handled accordingly.
}
\details{
Supply a user-defined error handling function to avoid stopping on error (for
example trying to delete a non-existing array). For example, use
\code{error=warning} to covert errors to warnings but still report them.

The package option \code{scidb.safe_remove} adds some protection for accidentally
removing arrays. When \code{TRUE} (the default value), only arrays that begin with
\code{R_array} are removed without also settin the \code{force} option.
Specify \code{force=TRUE} to remove any specified array, or optionally set the
\code{options(scidb.safe_remove=FALSE)} to disable this safety check.
}
\author{
B. W. Lewis<blewis@paradigm4.com>
}

\examples{
\dontrun{
data("iris")
x = as.scidb(iris)
# This is the SciDB array name corresponding to x:
print(x@name)

# Let's make an array with a different name:
y = as.scidb(iris, name="iris")
print(y@name)

# Removing everything will only delete arrays whose name begin with
# R_array by default:
scidbremove(scidblist())
scidblist()

# To remove the 'iris' array, use force=TRUE:
scidbremove("iris", force=TRUE)
scidblist()
}}
