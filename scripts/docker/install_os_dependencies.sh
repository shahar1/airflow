#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# shellcheck shell=bash
set -euo pipefail

if [[ "$#" != 1 ]]; then
    echo
    echo "ERROR! There should be 'runtime', 'ci' or 'dev' parameter passed as argument.".
    echo
    exit 1
fi

AIRFLOW_PYTHON_VERSION=${AIRFLOW_PYTHON_VERSION:-3.10.18}
PYTHON_LTO=${PYTHON_LTO:-true}
GOLANG_MAJOR_MINOR_VERSION=${GOLANG_MAJOR_MINOR_VERSION:-1.24.4}

if [[ "${1}" == "runtime" ]]; then
    INSTALLATION_TYPE="RUNTIME"
elif   [[ "${1}" == "dev" ]]; then
    INSTALLATION_TYPE="DEV"
elif   [[ "${1}" == "ci" ]]; then
    INSTALLATION_TYPE="CI"
else
    echo
    echo "ERROR! Wrong argument. Passed ${1} and it should be one of 'runtime', 'ci' or 'dev'.".
    echo
    exit 1
fi

function get_dev_apt_deps() {
    if [[ "${DEV_APT_DEPS=}" == "" ]]; then
        DEV_APT_DEPS="\
apt-transport-https \
apt-utils \
build-essential \
dirmngr \
freetds-bin \
freetds-dev \
git \
graphviz \
graphviz-dev \
gzip \
krb5-user \
lcov \
ldap-utils \
libbluetooth-dev \
libbz2-dev \
libc6-dev \
libdb-dev \
libev-dev \
libev4 \
libffi-dev \
libgdbm-compat-dev \
libgdbm-dev \
libgeos-dev \
libkrb5-dev \
libldap2-dev \
libleveldb-dev \
libleveldb1d \
liblzma-dev \
libncurses5-dev \
libreadline-dev \
libsasl2-2 \
libsasl2-dev \
libsasl2-modules \
libsqlite3-dev \
libssl-dev \
libxmlsec1 \
libxmlsec1-dev \
libzstd-dev \
locales \
lsb-release \
lzma \
lzma-dev \
openssh-client \
pkg-config \
pkgconf \
sasl2-bin \
sudo \
tk-dev \
unixodbc \
unixodbc-dev \
uuid-dev \
wget \
zlib1g-dev \
"
        export DEV_APT_DEPS
    fi
}

function get_runtime_apt_deps() {
    local debian_version
    local debian_version_apt_deps
    # Get debian version without installing lsb_release
    # shellcheck disable=SC1091
    debian_version=$(. /etc/os-release;   printf '%s\n' "$VERSION_CODENAME";)
    echo
    echo "DEBIAN CODENAME: ${debian_version}"
    echo
    debian_version_apt_deps="\
libffi8 \
libldap-2.5-0 \
libssl3 \
netcat-openbsd\
"
    echo
    echo "APPLIED INSTALLATION CONFIGURATION FOR DEBIAN VERSION: ${debian_version}"
    echo
    if [[ "${RUNTIME_APT_DEPS=}" == "" ]]; then
        RUNTIME_APT_DEPS="\
${debian_version_apt_deps} \
apt-transport-https \
apt-utils \
curl \
dumb-init \
freetds-bin \
git \
gnupg \
iputils-ping \
krb5-user \
ldap-utils \
libev4 \
libgeos-dev \
libsasl2-2 \
libsasl2-modules \
libxmlsec1 \
locales \
lsb-release \
openssh-client \
rsync \
sasl2-bin \
sqlite3 \
sudo \
unixodbc \
wget\
"
        export RUNTIME_APT_DEPS
    fi
}

function install_docker_cli() {
    apt-get update
    apt-get install ca-certificates curl
    install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc
    chmod a+r /etc/apt/keyrings/docker.asc
    # shellcheck disable=SC1091
    echo \
      "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian \
      $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
      tee /etc/apt/sources.list.d/docker.list > /dev/null
    apt-get update
    apt-get install -y --no-install-recommends docker-ce-cli
}

function install_debian_dev_dependencies() {
    if [[ ! -f /etc/shells ]]; then
        # The hardened image does not have /etc/shells, but tmux needs it.
        # We populate it with the shells available in the image.
        echo "/bin/bash" > /etc/shells
        echo "/bin/sh" >> /etc/shells
    fi
    apt-get update
    apt-get install -yqq --no-install-recommends apt-utils >/dev/null 2>&1
    apt-get install -y --no-install-recommends wget curl gnupg2 lsb-release ca-certificates
    # shellcheck disable=SC2086
    export ${ADDITIONAL_DEV_APT_ENV?}
    if [[ ${DEV_APT_COMMAND} != "" ]]; then
        bash -o pipefail -o errexit -o nounset -o nolog -c "${DEV_APT_COMMAND}"
    fi
    if [[ ${ADDITIONAL_DEV_APT_COMMAND} != "" ]]; then
        bash -o pipefail -o errexit -o nounset -o nolog -c "${ADDITIONAL_DEV_APT_COMMAND}"
    fi
    apt-get update
    local debian_version
    local debian_version_apt_deps
    # Get debian version without installing lsb_release
    # shellcheck disable=SC1091
    debian_version=$(. /etc/os-release;   printf '%s\n' "$VERSION_CODENAME";)
    echo
    echo "DEBIAN CODENAME: ${debian_version}"
    echo
    # shellcheck disable=SC2086
    groupadd -r sasl
    # shellcheck disable=SC2086
    apt-get install -y --no-install-recommends "${DEV_APT_DEPS}"
}

function install_additional_dev_dependencies() {
    if [[ "${ADDITIONAL_DEV_APT_DEPS=}" != "" ]]; then
        # shellcheck disable=SC2086
        apt-get install -y --no-install-recommends ${ADDITIONAL_DEV_APT_DEPS}
    fi
}

function link_python() {
    # link python binaries to /usr/local/bin and /usr/python/bin with and without 3 suffix
    # Links in /usr/local/bin are needed for tools that expect python to be there
    # Links in /usr/python/bin are needed for tools that are detecting home of python installation including
    # lib/site-packages. The /usr/python/bin should be first in PATH in order to help with the last part.
    mkdir -p /usr/local/bin
    for dst in pip3 python3 python3-config; do
        src="$(echo "${dst}" | tr -d 3)"
        echo "Linking ${dst} in /usr/local/bin and /usr/python/bin"
        ln -sv "/usr/python/bin/${dst}" "/usr/local/bin/${dst}"
        for dir in /usr/local/bin /usr/python/bin; do
            if [[ ! -e "${dir}/${src}" ]]; then
                echo "Creating ${src} - > ${dst} link in ${dir}"
                ln -sv "${dir}/${dst}" "${dir}/${src}"
            fi
        done
    done
    mkdir -p /usr/local/lib
    for dst in /usr/python/lib/*
    do
        src="/usr/local/lib/$(basename "${dst}")"
        if [[ -e "${src}" ]]; then
            rm -rf "${src}"
        fi
        echo "Linking ${dst} to ${src}"
        ln -sv "${dst}" "${src}"
    done
    ldconfig
}

function install_debian_runtime_dependencies() {
    apt-get update
    apt-get install --no-install-recommends -yqq apt-utils >/dev/null 2>&1
    apt-get install -y --no-install-recommends wget curl gnupg2 lsb-release ca-certificates
    # shellcheck disable=SC2086
    export ${ADDITIONAL_RUNTIME_APT_ENV?}
    if [[ "${RUNTIME_APT_COMMAND}" != "" ]]; then
        bash -o pipefail -o errexit -o nounset -o nolog -c "${RUNTIME_APT_COMMAND}"
    fi
    if [[ "${ADDITIONAL_RUNTIME_APT_COMMAND}" != "" ]]; then
        bash -o pipefail -o errexit -o nounset -o nolog -c "${ADDITIONAL_RUNTIME_APT_COMMAND}"
    fi
    apt-get update
    # shellcheck disable=SC2086
    apt-get install -y --no-install-recommends ${RUNTIME_APT_DEPS} ${ADDITIONAL_RUNTIME_APT_DEPS}
    apt-get autoremove -yqq --purge
    apt-get clean
    link_python
    rm -rf /var/lib/apt/lists/* /var/log/*
}

function install_python() {
    echo "Using pre-installed Python from base image"
    if [[ -d "/opt/python" ]]; then
        echo "Linking /opt/python to /usr/python"
        ln -snf /opt/python /usr/python
    else
        echo "ERROR: /opt/python not found in base image"
        exit 1
    fi
    link_python
}

function install_golang() {
    curl "https://dl.google.com/go/go${GOLANG_MAJOR_MINOR_VERSION}.linux-$(dpkg --print-architecture).tar.gz" -o "go${GOLANG_MAJOR_MINOR_VERSION}.linux.tar.gz"
    rm -rf /usr/local/go && tar -C /usr/local -xzf go"${GOLANG_MAJOR_MINOR_VERSION}".linux.tar.gz
}

function apt_clean() {
    apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false
    rm -rf /var/lib/apt/lists/* /var/log/*
}

if [[ "${INSTALLATION_TYPE}" == "RUNTIME" ]]; then
    get_runtime_apt_deps
    install_debian_runtime_dependencies
    install_docker_cli
    apt_clean
else
    get_dev_apt_deps
    install_debian_dev_dependencies
    install_python
    install_additional_dev_dependencies
    if [[ "${INSTALLATION_TYPE}" == "CI" ]]; then
        install_golang
    fi
    install_docker_cli
    apt_clean
fi
