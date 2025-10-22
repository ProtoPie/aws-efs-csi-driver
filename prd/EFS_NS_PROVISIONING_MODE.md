# EFS ns Provisioning Mode

새로운 provisioningMode 로써 네임스페이스(ns)별로 격리된 EFS(file system)를 생성하여 바인딩하는 모드입니다. 아래와 같이 `StorageClass`를 생성하여 사용할 수 있습니다.

```yaml
kind: StorageClass
apiVersion: storage.k8s.io/v1
metadata:
  name: efs-sc
provisioner: efs.csi.aws.com
parameters:
  provisioningMode: efs-ns
  directoryPerms: "700"
  gidRangeStart: "1000" # optional
  gidRangeEnd: "2000" # optional
  basePath: "/dynamic_provisioning" # optional
  az: us-east-1a
  csi.storage.k8s.io/provisioner-secret-name: x-account
  csi.storage.k8s.io/provisioner-secret-namespace: kube-system
```
