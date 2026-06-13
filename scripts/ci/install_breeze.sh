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
set -euxo pipefail

cd "$( dirname "${BASH_SOURCE[0]}" )/../../"

PYTHON_ARG=""

PIP_VERSION="26.1.2"
if [[ ${PYTHON_VERSION=} != "" ]]; then
    PYTHON_ARG="--python=$(which python"${PYTHON_VERSION}") "
fi

python -m pip install --upgrade "pip==${PIP_VERSION}"
uv tool uninstall apache-airflow-breeze >/dev/null 2>&1 || true
# shellcheck disable=SC2086
uv tool install ${PYTHON_ARG} --force --editable ./dev/breeze/
# Use $HOME/.local/bin (uv's default tool bin dir) rather than a hardcoded
# /home/runner/.local/bin: GitHub-hosted runners run as the `runner` user, but
# self-hosted runners (e.g. AWS CodeBuild) run as root, so breeze lands in
# /root/.local/bin. $HOME resolves correctly on both.
echo "${HOME}/.local/bin" >> "${GITHUB_PATH}"
