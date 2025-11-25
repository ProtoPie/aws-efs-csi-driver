# EFS-NS 프로비저닝 모드 구현 가이드

## 개요

`efs-ns` 프로비저닝 모드는 각 Kubernetes 네임스페이스별로 독립된 Amazon EFS 파일시스템을 동적으로 생성하고 관리하는 새로운 기능입니다. 기존 `efs-ap` 모드가 기존 파일시스템에 Access Point를 생성하는 방식과 달리, `efs-ns` 모드는 네임스페이스마다 완전히 격리된 파일시스템을 제공합니다.

## 현재 아키텍처 분석

### 기존 efs-ap 모드 동작 방식

1. **파라미터 검증**: `pkg/driver/controller.go:138-147`에서 `provisioningMode` 파라미터 확인
2. **파일시스템 ID 요구**: `fileSystemId` 파라미터가 필수로 제공되어야 함
3. **Access Point 생성**: 기존 EFS 파일시스템에 Access Point만 생성
4. **네임스페이스 정보 활용**: `PvcNamespace` 파라미터로 네임스페이스 정보 접근 가능

### CSI 파라미터 시스템

```go
// pkg/driver/controller.go:57-58
PvcName      = "csi.storage.k8s.io/pvc/name"
PvcNamespace = "csi.storage.k8s.io/pvc/namespace"
```

StorageClass 파라미터는 `volumeParams` 맵을 통해 접근하며, CSI 표준 파라미터들이 자동으로 주입됩니다.

## EFS-NS 구현을 위한 필수 요소들

### 1. 코드 수정 위치

#### A. Controller 서비스 (pkg/driver/controller.go)

**수정 필요 위치:**

- Line 41: `AccessPointMode` 상수 옆에 `NamespaceMode = "efs-ns"` 추가
- Line 141-143: 프로비저닝 모드 검증 로직 수정
- Line 149-161: 파일시스템 생성 로직 추가 (기존에는 `fileSystemId` 파라미터만 처리)

**새로 추가할 함수들:**

```go
func (d *controllerService) createFileSystemForNamespace(ctx context.Context, namespace string, volumeParams map[string]string) (string, error)
func (d *controllerService) getOrCreateFileSystemForNamespace(ctx context.Context, namespace string, volumeParams map[string]string) (string, error)
func generateFileSystemName(namespace, clusterName string) string
```

#### B. Cloud 인터페이스 확장 (pkg/cloud/cloud.go)

**현재 상태:** EFS 파일시스템 생성 기능 없음 (Access Point만 지원)

**추가 필요 메소드:**

```go
type Cloud interface {
    // 기존 메소드들...
    CreateFileSystem(ctx context.Context, opts *FileSystemOptions) (*FileSystem, error)
    DeleteFileSystem(ctx context.Context, fileSystemId string) error
    FindFileSystemByTags(ctx context.Context, tags map[string]string) (*FileSystem, error)
}
```

**추가 필요 구조체:**

```go
type FileSystemOptions struct {
    CreationToken    string
    Tags            map[string]string
    EncryptionConfig *EncryptionConfig
    ThroughputMode  string
    PerformanceMode string
}

type EncryptionConfig struct {
    Encrypted bool
    KmsKeyId  string
}
```

#### C. EFS 클라이언트 인터페이스 확장

**현재 Efs 인터페이스 (pkg/cloud/cloud.go:93-99):** Access Point 관련 메소드만 존재

**추가 필요 메소드:**

```go
type Efs interface {
    // 기존 메소드들...
    CreateFileSystem(context.Context, *efs.CreateFileSystemInput, ...func(*efs.Options)) (*efs.CreateFileSystemOutput, error)
    DeleteFileSystem(context.Context, *efs.DeleteFileSystemInput, ...func(*efs.Options)) (*efs.DeleteFileSystemOutput, error)
}
```

### 2. 파일시스템 명명 규칙

**제안하는 명명 규칙:**

- 파일시스템 이름: `{clusterName}-{namespace}-efs`
- Creation Token: `{clusterName}-{namespace}` (고유성 보장)
- 태그:
  - `Name`: 파일시스템 이름
  - `Namespace`: Kubernetes 네임스페이스
  - `KubernetesCluster`: 클러스터 이름
  - `CSIDriverVersion`: 드라이버 버전

### 3. 파일시스템 라이프사이클 관리

#### 생성 과정

1. `CreateVolume` 요청에서 네임스페이스 추출
2. 해당 네임스페이스용 파일시스템 존재 여부 확인
3. 없으면 새 파일시스템 생성 (태그 기반 검색)
4. 파일시스템 생성 후 Mount Target 대기
5. Access Point 생성 (기존 로직 재사용)

#### 삭제 과정

1. `DeleteVolume` 요청 처리
2. Access Point 삭제 (기존 로직)
3. **파일시스템 참조 카운팅 및 Finalizer 기반 삭제 관리** (하단 Finalizer 전략 참조)

### 4. 네트워킹 및 보안 고려사항

#### Mount Target 생성

```go
// 새로 추가해야 할 구조체와 메소드들
type MountTargetOptions struct {
    FileSystemId     string
    SubnetId         string
    SecurityGroupIds []string
}

func (c *cloud) CreateMountTargets(ctx context.Context, opts *MountTargetOptions) error
func (c *cloud) WaitForMountTargets(ctx context.Context, fileSystemId string) error
```

#### 보안 그룹 설정

- 기존 클러스터의 노드 보안 그룹 정보 활용
- NFS 트래픽 (포트 2049) 허용 규칙 자동 설정

### 5. StorageClass 파라미터 확장

**새로운 파라미터들:**

```yaml
parameters:
  provisioningMode: "efs-ns"
  performanceMode: "generalPurpose" # generalPurpose | maxIO
  throughputMode: "provisioned" # bursting | provisioned
  provisionedThroughputInMibps: "100"
  encrypted: "true"
  kmsKeyId: "alias/aws/elasticfilesystem"
```

### 6. IAM 권한 요구사항

**추가 필요 권한:**

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "elasticfilesystem:CreateFileSystem",
        "elasticfilesystem:DeleteFileSystem",
        "elasticfilesystem:CreateMountTarget",
        "elasticfilesystem:DeleteMountTarget",
        "elasticfilesystem:ModifyMountTargetSecurityGroups",
        "ec2:DescribeSubnets",
        "ec2:DescribeSecurityGroups",
        "ec2:DescribeVpcs"
      ],
      "Resource": "*"
    }
  ]
}
```

### 7. 에러 처리 및 복구

#### 예상 에러 상황들

1. **동시 생성 요청**: 같은 네임스페이스에서 여러 PVC 동시 생성
2. **네트워크 설정 실패**: Mount Target 생성 실패
3. **권한 부족**: EFS 생성 권한 없음
4. **할당량 초과**: AWS 계정의 EFS 파일시스템 한도 초과

#### 복구 전략

```go
// 제안하는 에러 처리 로직
func (d *controllerService) handleFileSystemCreationError(err error, namespace string) error {
    switch {
    case isFileSystemAlreadyExists(err):
        // 기존 파일시스템 재사용
        return d.findExistingFileSystem(namespace)
    case isInsufficientPermissions(err):
        return status.Errorf(codes.PermissionDenied, "Insufficient permissions to create EFS: %v", err)
    case isQuotaExceeded(err):
        return status.Errorf(codes.ResourceExhausted, "EFS quota exceeded: %v", err)
    default:
        return status.Errorf(codes.Internal, "Failed to create EFS: %v", err)
    }
}
```

### 8. 모니터링 및 로깅

**로깅 강화 포인트:**

- 파일시스템 생성/삭제 이벤트
- Mount Target 상태 변화
- 네임스페이스별 파일시스템 매핑

```go
klog.V(2).Infof("Creating EFS filesystem for namespace %s", namespace)
klog.V(5).Infof("FileSystem creation parameters: %+v", opts)
```

### 9. 테스팅 전략

#### 단위 테스트 추가

```go
// pkg/driver/controller_test.go에 추가할 테스트들
func TestCreateVolumeEfsNsMode(t *testing.T)
func TestCreateVolumeEfsNsModeExistingFileSystem(t *testing.T)
func TestCreateVolumeEfsNsModePermissionDenied(t *testing.T)
```

#### E2E 테스트 시나리오

1. 새 네임스페이스에 PVC 생성 → 새 EFS 생성 확인
2. 동일 네임스페이스에 추가 PVC 생성 → 기존 EFS 재사용 확인
3. 다른 네임스페이스에 PVC 생성 → 별도 EFS 생성 확인

### 10. 성능 및 비용 최적화

#### 파일시스템 재사용 로직

```go
func (d *controllerService) findOrCreateFileSystemForNamespace(ctx context.Context, namespace string) (string, error) {
    // 1. 태그 기반으로 기존 파일시스템 검색
    // 2. 없으면 새로 생성
    // 3. 캐싱으로 중복 검색 방지
}
```

#### 캐싱 전략

- 네임스페이스별 파일시스템 ID를 메모리에 캐시
- TTL 기반 캐시 무효화
- 클러스터 재시작시 캐시 복구

## 구현 우선순위

### Phase 1: 기본 기능

1. Controller 서비스에서 efs-ns 모드 인식
2. Cloud 인터페이스에 파일시스템 생성 기능 추가
3. 기본적인 파일시스템 생성 로직

### Phase 2: 고급 기능

1. Mount Target 자동 생성
2. 보안 그룹 자동 설정
3. 에러 처리 및 복구 로직

### Phase 3: 최적화

1. 파일시스템 재사용 및 캐싱
2. 모니터링 및 메트릭스
3. 성능 튜닝

## 잠재적 위험 및 고려사항

1. **비용**: 네임스페이스당 독립 EFS → 비용 증가
2. **복잡성**: Mount Target, 보안 그룹 관리 복잡도 증가
3. **삭제 정책**: 파일시스템 삭제 시점 결정 어려움
4. **네트워크 격리**: VPC/서브넷 설정에 따른 접근성 이슈
5. **할당량**: AWS 계정당 EFS 파일시스템 수 제한

이 가이드를 바탕으로 단계적으로 `efs-ns` 기능을 구현할 수 있으며, 각 단계에서 충분한 테스트와 검증을 통해 안정성을 확보하는 것이 중요합니다.
