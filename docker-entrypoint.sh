#!/bin/sh
set -e

# Copy EFS static files if they exist and target doesn't exist
if [ -d "/etc/amazon/efs-static-files" ] && [ ! -f "/etc/amazon/efs/efs-utils.conf" ]; then
    echo "Copying EFS static configuration files..."
    cp -r /etc/amazon/efs-static-files/* /etc/amazon/efs/ 2>/dev/null || true
fi

# Ensure log directory permissions are correct
if [ -d "/var/log/amazon/efs" ]; then
    chmod -R 777 /var/log/amazon/efs 2>/dev/null || true
fi

# Ensure runtime directory permissions are correct
if [ -d "/var/run/efs" ]; then
    chmod -R 777 /var/run/efs 2>/dev/null || true
fi

# Execute the main binary
exec /bin/aws-efs-csi-driver "$@"