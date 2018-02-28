spark_dependencies <- function(spark_version, scala_version, ...){
  sparklyr::spark_dependency(packages = "com.github.master:spark-stemming_2.10:0.2.0")
}

.onLoad <- function(libname, pkgname) {
  sparklyr::register_extension(pkgname)
}


