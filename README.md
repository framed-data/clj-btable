## BTable

A fast, compact binary serialization format for sparse, labeled 2D numeric datasets ('binary tables').

### Motivations

Prior to BTables, we were storing large sparse 2D datasets in dense CSVs, which is highly space- and
performance-inefficient. We looked into [HDF5](http://www.hdfgroup.org/HDF5/) although found it to be
overly complex for our use case, and early investigation did not yield compelling gains in performance
or space. Thus BTables was designed to be a simple, fast, and compact format to represent sparse numeric
datasets.

A BTable is basically a binary representation of a sparse matrix on disk, and the format is inspired
by the [Compressed Row Storage](http://netlib.org/linalg/html_templates/node91.html) (CRS) format,
saving space by only storing the indices/values of nonzero cells. It is designed in a strictly
row-oriented format for efficient iteration, and is _not_ a library for matrix computation or
linear algebra.

Note that BTables are *not* a drop-in replacement for all datasets stored as CSV:
the increases in efficiency is proportional to the sparsity of the dataset.
For a pathological fully-nonzero dataset, the space occupied can be much larger than a CSV!

### Examples

```clj
(require '[clj-btable.core :as btable])

(def labels ["login", "view_item", "purchase"])
(def rows [[5.0 3.0 1.0] [2.0 0.0 0.0] [0.0 0.0 0.0]])
(btable/write "out.btable" labels rows)
; #<File out.btable>

(btable/labels "out.btable")
; => ["login", "view_item", "purchase"]

(doseq [row (btable/rows "out.btable")]
  ; Process a single row in a lazy sequence of rows
  )
```

Also see [the documentation](http://framed-data.github.io/clj-btable).

### Disk format

See [the wiki](https://github.com/framed-data/clj-btable/wiki/Disk-Format) for a detailed description of the representation on disk.


### Performance

An optimized Java backend using NIO can write a table of 50,000 rows, each with 500 columns (25 million cells) in just under 8 seconds.
The same table can be read/traversed in ~3s.
