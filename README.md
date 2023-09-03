[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/htrc/Metadata-combine-features-meta/ci.yml?branch=main)](https://github.com/htrc/Metadata-combine-features-meta/actions/workflows/ci.yml)
[![codecov](https://codecov.io/github/htrc/Metadata-bibframe2jsonld/graph/badge.svg?token=otalCjsK9m)](https://codecov.io/github/htrc/Metadata-bibframe2jsonld)
[![GitHub release (latest SemVer including pre-releases)](https://img.shields.io/github/v/release/htrc/Metadata-combine-features-meta?include_prereleases&sort=semver)](https://github.com/htrc/Metadata-combine-features-meta/releases/latest)

# Metadata-bibframe2jsonld
Used to convert enriched BIBFRAME-XML to HTRC metadata JSONLD

# Build
* To generate a package that can be invoked via a shell script, run:  
  `sbt stage`  
  then find the result in `target/universal/stage/` folder.
* To generate a distributable ZIP package, run:  
  `sbt dist`  
  then find the result in `target/universal/` folder.

# Run
```
combine-features-metadata
  -f, --features  <arg>       (Optional) The path to the folder containing the
                              features data
  -l, --log-level  <LEVEL>    (Optional) The application log level; one of INFO,
                              DEBUG, OFF (default = INFO)
  -m, --metadata  <arg>       (Optional) The path to the folder containing the
                              metadata
  -n, --num-partitions  <N>   (Optional) The number of partitions to split the
                              input set of HT IDs into, for increased
                              parallelism
  -o, --output  <DIR>         Write the output to DIR
  -s, --save-as-seq           (Optional) Saves the EF files as Hadoop sequence
                              files
      --spark-log  <FILE>     (Optional) Where to write logging output from
                              Spark to
  -h, --help                  Show help message
  -v, --version               Show version of this program
```
