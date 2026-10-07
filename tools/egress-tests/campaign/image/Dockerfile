# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# The campaign image: campaign.py, plot/fit.py and the egress-tests driver,
# with numpy and kubectl, run by deploy.sh --campaign. build.sh stages the
# context; see tools/egress-tests/README.md.

FROM python:3.13-slim@sha256:bf44cdfcb76cd3b41e879bc058fc37ec5872002ccfde7fcb765e218cde0cd79c

ARG KUBECTL_VERSION=v1.35.9
ARG KUBECTL_SHA256=3cfeaf80be482b435b0aa214aff6e0b2c312ee23c0ff20810c75517b6004c6eb

# The root filesystem is read-only in the Pod; /tmp is an emptyDir.
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 HOME=/tmp

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt && rm /tmp/requirements.txt

ADD https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl /usr/local/bin/kubectl
RUN echo "${KUBECTL_SHA256}  /usr/local/bin/kubectl" | sha256sum -c - && chmod 0755 /usr/local/bin/kubectl

WORKDIR /work
COPY egress-tests /work/egress-tests
COPY campaign/ /work/campaign/
COPY plot/ /work/plot/
COPY ref/ /work/ref/

USER 65532:65532
