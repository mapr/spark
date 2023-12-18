#!/bin/sh
new_version=$1

if [ -z "$new_version" ]; then
    echo "New version can't be empty" >&2
    exit 1
fi

old_version=$(mvn -q -Dexec.executable=echo -Dexec.args='${project.version}' --non-recursive exec:exec)
echo "$old_version"

find . -type f -name pom.xml -exec sed -i "s|<version>${old_version}</version>|<version>${new_version}</version>|" {} \;
