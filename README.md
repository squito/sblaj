SBLAJ
=====

SBLAJ -- Sparse Basic Linear Algebra for the JVM -- is a library that provides standard operations on *sparse* matrices,
with a focus on operations required for Machine Learning.  The core goals are:

* Extremeley memory efficient for large, sparse matrices
    * Optimized storage for boolean data
    * Handles > billions of entries
* Clean API for common matrix operations
* Cache-friendly storage
* Fast -- competitive with application specific code
* Compact data formats -- small on disk and fast to load

The extended packages also provide:

* Implementations of standard machine learning algorithms that are designed for large, sparse data, including
    * Naive Bayes
    * Stochastic Gradient Descent
    * LDA
* Utilities to get data into the Sparse Matrix Formats
* Compatability with Hadoop and Spark, for distributed computing

Getting Started
----

SBLAJ can be built using sbt.  Simply check out the source and run

```
sbt package
```

to create a usable jar.  You can try out the code by running the units tests from within sbt, either with
`test` to run all the tests, or eg. `~test-only org.sblaj.BaseSparseBinaryVectorTest` to repeatedly
run one set of tests as you modify the code

Current Status
----

SBLAJ is still pre-0.1.  Some basic ideas are in place, but the API is still in flux.  It is actively developed,
and the intention is to be a well-tested, robust library, not simply a toy demonstrating one idea.  Contributions
and ideas from others would be greatly appreciated!

Roadmap
----

#### v0.1

1. SparseBinaryRowMatrix -- iterators & helper functions
2. NaiveBayes
3. Matrix Creation Utils
    * Clean api to featurize, creating header, matrix, and dictionary
    * transform "hashed" to "enumerated" features

#### future (rough prioritization)

* SGD
* Hadoop reader / writer
* Matrix Market Import & Export
* Spark API
* Timing Comparisons
* LDA
* Integration with Scalala
* Std filters & subsets, eg. min occurrence, min correlation, etc.

* Additional Matrix Types
    * Float, Column

* Varint encoded matrices
* Views -- api to index sub-portions of matrix, with lazy & force version
* Mixed-mode matrices
    * dense float + sparse binary (eg., time series)
* HMM implementation
