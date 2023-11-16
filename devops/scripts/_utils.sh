#!/bin/bash

SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}")
. "${SCRIPT_DIR}/_initialize_package_variables.sh"

OS="redhat"
if [ -e "/etc/debian_version" ]; then
  OS="debian"
elif [ -e "/etc/SuSE-release" ] || grep -q "ID_LIKE.*suse" "/etc/os-release" 2>/dev/null; then
  OS="suse"
fi

build_main() {
  ROLE_NAME="$1"

  opt_mapr="${BUILD_DIR}/root/${ROLE_NAME}${INSTALLATION_PREFIX}"
  pkg_home="${BUILD_DIR}/root/${ROLE_NAME}${PKG_INSTALL_ROOT}"

  mkdir -p "$pkg_home"

  mv -T "${BUILD_DIR}/build" "$pkg_home"

  echo "$PKG_3DIGIT_VERSION" > "${opt_mapr}/${PKG_NAME}/${PKG_NAME}version"
  ln -sr "$pkg_home" "${opt_mapr}/${PKG_NAME}/current"
}

build_role() {
  ROLE_NAME="$1"

  opt_mapr="${BUILD_DIR}/root/${ROLE_NAME}${INSTALLATION_PREFIX}"

  mkdir -p "${opt_mapr}/roles"
  find "devops/specs/${ROLE_NAME}/roles/" -type f -exec cp {} "${opt_mapr}/roles" \;
  _replace_build_variables "${opt_mapr}/roles"
}

build_package() {
  if [ "$OS" = "debian" ]; then
    _build_deb $@
  else
    _build_rpm $@
  fi
}

_build_rpm() {
  ROLE_NAME="$1"

  rpm_root="${BUILD_DIR}/package/${ROLE_NAME}/rpm"

  mkdir -p "${rpm_root}/SOURCES"
  mv -T "${BUILD_DIR}/root/${ROLE_NAME}" "${rpm_root}/SOURCES"

  mkdir -p "${rpm_root}/SPECS"
  cp devops/specs/${ROLE_NAME}/rpm/*.spec "${rpm_root}/SPECS"
  _replace_build_variables "${rpm_root}/SPECS"

  rpmbuild --bb --define "_topdir ${rpm_root}" --buildroot="${rpm_root}/SOURCES" ${rpm_root}/SPECS/*
  mkdir -p "$DIST_DIR"
  mv ${rpm_root}/RPMS/*/*rpm "$DIST_DIR"
}

_build_deb() {
  ROLE_NAME="$1"

  deb_root="${BUILD_DIR}/package/${ROLE_NAME}/deb"

  mkdir -p "$deb_root"
  mv -T "${BUILD_DIR}/root/${ROLE_NAME}" "${deb_root}"

  mkdir -p "${deb_root}/DEBIAN"
  cp devops/specs/${ROLE_NAME}/deb/* "${deb_root}/DEBIAN"
  _replace_build_variables "${deb_root}/DEBIAN"

  find "$deb_root" -type f -exec md5sum \{\} \; 2>/dev/null |
    sed -e "s|${deb_root}||" -e "s| \/| |" |
    grep -v DEBIAN > "${deb_root}/DEBIAN/md5sums"
  echo "" >> "${deb_root}/DEBIAN/control"

  mkdir -p "$DIST_DIR"
  dpkg-deb --build "$deb_root" "$DIST_DIR"
}

_replace_build_variables() {
  REPLACE_DIR="$1"
  find "$REPLACE_DIR" -type f \
    -exec sed -i \
      -e "s|__PREFIX__|${INSTALLATION_PREFIX}|g" \
      -e "s|__VERSION__|${PKG_VERSION}|g" \
      -e "s|__VERSION_3DIGIT__|${PKG_3DIGIT_VERSION}|g" \
      -e "s|__RELEASE_BRANCH__|${PACKAGE_INFO_BRANCH}|g" \
      -e "s|__RELEASE_VERSION__|${PKG_VERSION}.${TIMESTAMP}|g" \
      -e "s|__INSTALL_3DIGIT__|${PKG_INSTALL_ROOT}|g" \
      -e "s|__GIT_COMMIT__|${GIT_COMMIT}|g" \
    {} \;
}
