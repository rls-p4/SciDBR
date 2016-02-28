# A general SciDB array class for R. It's a hybrid S4 class with some S3
# methods. The class can represent SciDB arrays and array promises.
#
# A scidb object is fully defined by:
# name = any SciDB expression that can produce an array 
# meta = environment containing SciDB schema and logical_plan character values (lazily evaluated)
# gc = environment
#      If gc$remove = TRUE, remove SciDB array when R gc is run on object.
#      The gc environment also stores dependencies required by array promises.

setClass("scidb",
         representation(name="character",
                        meta="environment",
                        gc="environment"),
         S3methods=TRUE)
