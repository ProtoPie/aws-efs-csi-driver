# Copyright 2019 The Kubernetes Authors.
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

FROM golang:alpine as go-builder
RUN apk add --no-cache git make gcc musl-dev
WORKDIR /go/src/github.com/kubernetes-sigs/aws-efs-csi-driver

ARG TARGETOS
ARG TARGETARCH
RUN echo "TARGETOS:$TARGETOS, TARGETARCH:$TARGETARCH"
RUN echo "I am running on $(uname -s)/$(uname -m)"

ADD . .

# Default client source is `k8s` which can be overriden with –-build-arg when building the Docker image
ARG client_source=k8s
ENV EFS_CLIENT_SOURCE=$client_source

RUN OS=${TARGETOS} ARCH=${TARGETARCH} make $TARGETOS/$TARGETARCH

FROM amazonlinux:2023 as rpm-provider

# Install Python 3 and required packages
RUN yum -y install python3 python3-pip gcc

# Install efs-utils directly from the repository
RUN mkdir -p /tmp/rpms && \
    yum -y install amazon-efs-utils --downloadonly --downloaddir=/tmp/rpms && \
    yum clean all

# Install botocore required by efs-utils for cross account mount
RUN pip3 install --user botocore

# This image is equivalent to the eks-distro-minimal-base-python image but with pip installed as well
FROM amazonlinux:2023 as rpm-installer

RUN yum -y install python3 python3-pip nfs-utils stunnel

COPY --from=rpm-provider /tmp/rpms/* /tmp/download/

# Install amazon-efs-utils RPM
RUN yum -y localinstall /tmp/download/*.rpm && \
    yum clean all

# At image build time, static files installed by efs-utils in the config directory, i.e. CAs file, need
# to be saved in another place so that the other stateful files created at runtime, i.e. private key for
# client certificate, in the same config directory can be persisted to host with a host path volume.
# Otherwise creating a host path volume for that directory will clean up everything inside at the first time.
# Those static files need to be copied back to the config directory when the driver starts up.
RUN if [ -d /etc/amazon/efs ]; then mv /etc/amazon/efs /etc/amazon/efs-static-files; fi

FROM amazonlinux:2023 AS linux-amazon

# Install runtime dependencies
RUN yum -y install python3 python3-pip nfs-utils stunnel && \
    yum clean all

# Copy installed packages and files from rpm-installer
COPY --from=rpm-installer /usr /usr
COPY --from=rpm-installer /etc /etc

# Copy botocore from rpm-provider
COPY --from=rpm-provider /root/.local/lib/python3.*/site-packages/ /usr/lib/python3.11/site-packages/

# Copy the built driver binary
COPY --from=go-builder /go/src/github.com/kubernetes-sigs/aws-efs-csi-driver/bin/aws-efs-csi-driver /bin/aws-efs-csi-driver
COPY THIRD-PARTY /

# Create necessary directories for EFS utilities with proper permissions
# Note: These need to be writable as the CSI driver runs as non-root in production
RUN mkdir -p /var/log/amazon/efs && \
    chmod 777 /var/log/amazon/efs && \
    mkdir -p /var/run/efs && \
    chmod 777 /var/run/efs && \
    mkdir -p /etc/amazon/efs && \
    chmod 755 /etc/amazon/efs && \
    mkdir -p /var/amazon/efs && \
    chmod 777 /var/amazon/efs && \
    # Create default log files to ensure they exist
    touch /var/log/amazon/efs/mount-watchdog.log && \
    chmod 666 /var/log/amazon/efs/mount-watchdog.log && \
    touch /var/log/amazon/efs/mount.log && \
    chmod 666 /var/log/amazon/efs/mount.log

# Create a symbolic link for stunnel5 to stunnel (for backward compatibility)
RUN if [ -f /usr/bin/stunnel ]; then ln -s /usr/bin/stunnel /usr/bin/stunnel5; fi

# Copy and set entrypoint script
COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

ENTRYPOINT ["/docker-entrypoint.sh"]
