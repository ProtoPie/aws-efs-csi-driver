# efs-ns 프로비저닝 모드 요구사항 문서

## 소개

efs-ns 프로비저닝 모드는 AWS EFS CSI Driver에 추가되는 새로운 동적 프로비저닝 기능입니다. 기존의 efs-ap 모드가 기존 EFS 파일시스템에 Access Point를 생성하는 방식과 달리, efs-ns 모드는 각 Kubernetes 네임스페이스별로 독립적인 Amazon EFS 파일시스템을 자동으로 생성하고 관리합니다. 이는 네임스페이스 간의 완전한 스토리지 격리를 제공하며, 멀티테넌트 환경에서의 보안과 리소스 관리를 향상시킵니다.

## 요구사항

### 요구사항 1: 네임스페이스별 EFS 파일시스템 자동 생성

**사용자 스토리:** Kubernetes 관리자로서, 새로운 네임스페이스에서 efs-ns 모드 StorageClass를 사용하는 PVC가 생성될 때 해당 네임스페이스 전용 EFS 파일시스템이 자동으로 생성되기를 원합니다. 이를 통해 네임스페이스 간의 완전한 스토리지 격리를 확보할 수 있습니다.

#### 승인 기준

1. WHEN 네임스페이스에서 efs-ns 모드 StorageClass를 사용하는 PVC가 처음 생성될 때 THEN CSI Driver SHALL 해당 네임스페이스 전용 새로운 EFS 파일시스템을 AWS EFS API를 통해 생성한다
2. WHEN EFS 파일시스템이 생성될 때 THEN 파일시스템 이름 SHALL "efs-ns-{namespace-name}-{cluster-id}" 형식으로 명명된다
3. WHEN 파일시스템이 생성될 때 THEN 적절한 태그 SHALL 자동으로 추가된다 (네임스페이스, 클러스터 정보 포함)
4. IF 동일한 네임스페이스에서 두 번째 PVC가 생성될 때 THEN 기존 파일시스템을 재사용 SHALL 한다

### 요구사항 2: StorageClass 파라미터 지원

**사용자 스토리:** Kubernetes 관리자로서, StorageClass에서 EFS 파일시스템 생성을 위한 다양한 파라미터를 설정할 수 있기를 원합니다. 이를 통해 성능, 보안, 비용 요구사항에 맞는 파일시스템을 생성할 수 있습니다.

#### 승인 기준

1. WHEN StorageClass에 provisioningMode: efs-ns가 설정될 때 THEN CSI Driver SHALL efs-ns 모드로 동작한다
2. WHEN performanceMode 파라미터가 제공될 때 THEN 파일시스템 SHALL 지정된 성능 모드(generalPurpose 또는 maxIO)로 생성된다
3. WHEN throughputMode 파라미터가 제공될 때 THEN 파일시스템 SHALL 지정된 처리량 모드(bursting 또는 provisioned)로 생성된다
4. WHEN encrypted 파라미터가 true로 설정될 때 THEN 파일시스템 SHALL 암호화된 상태로 생성된다
5. IF provisioned throughput 모드가 설정될 때 THEN provisionedThroughputInMibps 파라미터 SHALL 함께 제공되어야 한다

### 요구사항 3: 볼륨 마운팅 및 언마운팅

**사용자 스토리:** 애플리케이션 개발자로서, efs-ns 모드로 생성된 PVC를 Pod에 마운트하고 언마운트할 수 있기를 원합니다. 기존 EFS 마운팅 방식과 동일한 사용자 경험을 제공받고 싶습니다.

#### 승인 기준

1. WHEN Pod가 efs-ns PVC를 마운트 요청할 때 THEN CSI Driver SHALL 해당 네임스페이스의 EFS 파일시스템을 EFS Helper를 통해 마운트한다
2. WHEN 마운트가 수행될 때 THEN encryptInTransit 설정 SHALL 적용된다
3. WHEN 여러 Pod가 동일한 PVC를 마운트할 때 THEN ReadWriteMany 액세스 모드 SHALL 지원된다
4. WHEN Pod가 종료될 때 THEN 볼륨은 자동으로 언마운트 SHALL 된다

### 요구사항 4: 네임스페이스 리소스 정리

**사용자 스토리:** Kubernetes 관리자로서, 네임스페이스가 삭제되거나 네임스페이스 내의 모든 efs-ns PVC가 삭제될 때 연관된 EFS 파일시스템이 적절히 정리되기를 원합니다. 불필요한 AWS 리소스 비용을 방지하고 싶습니다.

#### 승인 기준

1. WHEN 네임스페이스 내의 마지막 efs-ns PVC가 삭제될 때 THEN CSI Driver SHALL 해당 EFS 파일시스템을 AWS에서 삭제한다
2. WHEN 파일시스템 삭제가 요청될 때 THEN 파일시스템에 마운트 타겟이 존재하는 경우 먼저 삭제 SHALL 한다
3. IF 파일시스템 삭제 중 오류가 발생할 때 THEN 적절한 오류 메시지와 함께 이벤트 SHALL 기록된다
4. WHEN 네임스페이스가 삭제될 때 AND PVC가 여전히 존재할 때 THEN finalizer를 통해 파일시스템 정리가 완료될 때까지 삭제를 지연 SHALL 한다

### 요구사항 5: 보안 및 네트워크 격리

**사용자 스토리:** 보안 관리자로서, 각 네임스페이스의 EFS 파일시스템이 다른 네임스페이스로부터 격리되고 적절한 보안 제어가 적용되기를 원합니다. 멀티테넌트 환경에서의 데이터 보안을 보장하고 싶습니다.

#### 승인 기준

1. WHEN EFS 파일시스템이 생성될 때 THEN 네임스페이스별 고유한 보안 그룹 SHALL 생성되어 적용된다
2. WHEN 마운트 타겟이 생성될 때 THEN 클러스터의 VPC 서브넷에만 생성 SHALL 된다
3. WHEN 암호화가 활성화될 때 THEN KMS 키를 사용한 저장 시 암호화 SHALL 적용된다
4. WHEN 전송 중 암호화가 활성화될 때 THEN TLS를 통한 EFS 마운트 SHALL 수행된다

### 요구사항 6: 메트릭 및 모니터링

**사용자 스토리:** 시스템 관리자로서, efs-ns 모드로 생성된 파일시스템의 사용량, 성능, 상태를 모니터링할 수 있기를 원합니다. 리소스 사용량을 추적하고 문제를 조기에 감지하고 싶습니다.

#### 승인 기준

1. WHEN 볼륨 메트릭이 활성화될 때 THEN 네임스페이스별 파일시스템 사용량 메트릭 SHALL 수집된다
2. WHEN 파일시스템이 생성되거나 삭제될 때 THEN Kubernetes 이벤트 SHALL 기록된다
3. WHEN 오류가 발생할 때 THEN 적절한 로그 레벨로 구조화된 로그 SHALL 생성된다
4. WHEN CSI Driver가 준비 상태 점검을 받을 때 THEN EFS API 연결 상태 SHALL 검증된다

### 요구사항 7: 호환성 및 마이그레이션

**사용자 스토리:** 플랫폼 엔지니어로서, 기존 EFS CSI Driver 설치 환경에서 efs-ns 모드를 추가로 활성화할 수 있기를 원합니다. 기존 워크로드에 영향을 주지 않으면서 새로운 기능을 점진적으로 도입하고 싶습니다.

#### 승인 기준

1. WHEN efs-ns 모드가 활성화될 때 THEN 기존 efs-ap 모드 동작 SHALL 영향받지 않는다
2. WHEN 여러 프로비저닝 모드가 동시에 사용될 때 THEN 각각 독립적으로 동작 SHALL 한다
3. WHEN 드라이버가 업그레이드될 때 THEN 기존 efs-ns 볼륨 SHALL 계속 작동한다
4. IF 네임스페이스에 기존 EFS 볼륨이 있을 때 THEN efs-ns 볼륨과 공존 SHALL 가능하다

### 요구사항 8: 오류 처리 및 복구

**사용자 스토리:** 플랫폼 운영자로서, EFS 파일시스템 생성, 삭제, 마운팅 과정에서 발생하는 다양한 오류 상황에 대해 명확한 피드백을 받고 적절한 복구 메커니즘이 동작하기를 원합니다.

#### 승인 기준

1. WHEN AWS API 호출이 실패할 때 THEN 구체적인 오류 원인과 함께 CSI 오류 SHALL 반환된다
2. WHEN 파일시스템 생성이 부분적으로 실패할 때 THEN 생성된 리소스 SHALL 자동으로 정리된다
3. WHEN 네트워크 오류로 마운트가 실패할 때 THEN 재시도 메커니즘 SHALL 동작한다
4. IF 파일시스템이 AWS에서 수동으로 삭제될 때 THEN PVC 삭제 시 graceful하게 처리 SHALL 된다
