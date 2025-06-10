`BUILD_DIR=devops/buildroot` - root dir for everything

```
>   devops
>   └── buildroot
```


## Build your project
Build your project and put build restuls in `${BUILD_DIR}/build`.

```
    devops
    └── buildroot
>       └── build
>           ├── bin
>           ├── conf
>           ├── ...
```

## Prepare directory sctructure of roles

Prepare directory structure for roles in `${BUILD_DIR}/root`.

```
    devops
    └── buildroot
        ├── build
        │   ├── bin
        │   ├── conf
        │   ├── ...
        └── root
>           ├ mapr-spark
>           │   └── opt
>           │       └── mapr
>           │           └── roles
>           │               └── spark
>           └ mapr-spark-thriftserver
>               └── opt
>                   └── mapr
>                       └── roles
>                           └── spark-thriftserver
```

## Prepare directory structure of the main package

Prepare directory structure of the main package and put build results in corresponding directory.

```
    devops
    └── buildroot
        ├── build
        │   ├── bin
        │   ├── conf
        │   ├── ...
        └── root
            ├ mapr-spark
            │   └── opt
            │       └── mapr
            │           ├── roles
            │           │   └── spark
>           │           └── spark
>           │               ├── spark-<version>
>           │               │   ├── bin
>           │               │   ├── conf
>           │               │   ├── ...
>           │               └── sparkversion
            └ mapr-spark-thriftserver
                └── opt
                    └── mapr
                        └── roles
                            └── spark
```


## Setup directory structure for RPM/DEB packages and build them

Setup directory structure for RPM/DEB packages in `${BUILD_DIR}/package` and build them.

Example for DEB package:
```
    devops
    └── buildroot
        ├── build
        │   ├── bin
        │   ├── conf
        │   ├── ...
        ├── root
        │   ├ mapr-spark
        │   │   └── opt
        │   │       └── mapr
        │   │           ├── roles
        │   │           │   └── spark
        │   │           └── spark
        │   │               ├── spark-<version>
        │   │               │   ├── bin
        │   │               │   ├── conf
        │   │               │   ├── ...
        │   │               └── sparkversion
        │   └ mapr-spark-thriftserver
        │       └── opt
        │           └── mapr
        │               └── roles
        │                   └── spark
>       └── package
>           ├── mapr-spark
>           │   └── deb
>           │       ├── opt
>           │       │   └── mapr
>           │       │       ├── spark
>           │       │       │   ├── spark-<version>
>           │       │       │   │   ├── bin
>           │       │       │   │   ├── conf
>           │       │       │   │   ├── ...
>           │       │       │   └── sparkversion
>           │       │       └── roles
>           │       │           └── spark
>           │       └── DEBIAN
>           │           ├── control
>           │           ├── postinst
>           │           ├── ...
>           └── mapr-spark-historyserver
>               └── deb
>                   ├── opt
>                   │   └── mapr
>                   │       └── roles
>                   │           └── spark
>                   └── DEBIAN
>                       ├── control
>                       ├── postinst
>                       ├── ...
```

Example for RPM package:
```
    devops
    └── buildroot
        ├── build
        │   ├── bin
        │   ├── conf
        │   ├── ...
        ├── root
        │   ├ mapr-spark
        │   │   └── opt
        │   │       └── mapr
        │   │           ├── roles
        │   │           │   └── spark
        │   │           └── spark
        │   │               ├── spark-<version>
        │   │               │   ├── bin
        │   │               │   ├── conf
        │   │               │   ├── ...
        │   │               └── sparkversion
        │   └ mapr-spark-thriftserver
        │       └── opt
        │           └── mapr
        │               └── roles
        │                   └── spark
>       └── package
>           ├── mapr-spark
>           │   └── rpm
>           │       ├── SOURCES
>           │       │   └── opt
>           │       │       └── mapr
>           │       │           ├── spark
>           │       │           │   ├── spark-<version>
>           │       │           │   │   ├── bin
>           │       │           │   │   ├── conf
>           │       │           │   │   ├── ...
>           │       │           │   └── sparkversion
>           │       │           └── roles
>           │       │               └── spark
>           │       └── SPECS
>           │           └── mapr-spark.spec
>           └── mapr-spark-historyserver
>               └── rpm
>                   ├── SOURCES
>                   │   └── opt
>                   │       └── mapr
>                   │           └── roles
>                   │               └── spark-historyserver
>                   └── SPECS
>                       └── mapr-spark-historyserver.spec
```
