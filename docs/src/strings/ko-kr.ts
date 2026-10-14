import type { Strings } from "../i18n";

export default {
  site: {
    nav: "사이트",
    examples: "예제",
    language: "언어",
    closeNav: "탐색 메뉴 닫기",
    license: "MIT 라이선스",
    editPage: "GitHub에서 이 페이지 편집",
    llmsFooter: "AI 에이전트용 llms.txt",
  },
  theme: {
    label: "테마: {mode}. {next} 모드로 전환하세요.",
    auto: "자동",
    light: "라이트",
    dark: "다크",
  },
  code: {
    copy: "복사",
    copied: "복사됨",
    copyFailed: "복사 실패",
    variants: "코드 변형",
    sectionLink: "이 섹션 링크",
  },
  docs: {
    title: "ck-orm — TypeScript용 타입 안전 ClickHouse",
    description:
      "ck-orm은 최신 JavaScript 런타임용 타입이 지정된 ClickHouse 쿼리 레이어입니다. 스키마 DSL, 쿼리 빌더, 원시 SQL 템플릿, 세션, 옵저버빌리티를 제공합니다.",
    skip: "레퍼런스로 건너뛰기",
    tocLabel: "레퍼런스",
    openNav: "레퍼런스 탐색 메뉴 열기",
    navLabel: "레퍼런스 탐색",
    badge: "0.1.0 이전에는 릴리스에서 API 호환성이 깨질 수 있습니다. 정확한 버전을 고정하세요.",
    headline: "타입 안전 ClickHouse, 감추지 않습니다",
    intro:
      "ck-orm은 최신 JavaScript 런타임용 ClickHouse 스키마 DSL, 타입이 지정된 쿼리 빌더, SQL 템플릿 레이어입니다. 쿼리를 읽기 쉬운 ClickHouse SQL로 컴파일하고, 값을 타입이 지정된 매개변수로 전달합니다. 64비트 정수와 Decimal은 정확한 문자열로 디코딩합니다.",
    ctaInstall: "설치부터 시작하기",
    ctaExamples: "예제 보기",
  },
  examples: {
    title: "ck-orm 예제",
    description: "ck-orm 예제를 실행하고 각 예제가 ClickHouse에 전송하는 SQL을 그대로 확인하세요.",
    skip: "예제로 건너뛰기",
    tocLabel: "예제",
    openNav: "예제 탐색 메뉴 열기",
    navLabel: "예제 탐색",
    heading: "예제",
    intro:
      "각 예제는 {folder}에 있는 실행 가능한 프로젝트입니다. 프로젝트에는 {main}과 {package}이 있습니다. 이 페이지는 그 {main} 파일을 그대로 보여 줍니다. 빌드가 이 파일을 ck-orm으로 실행하므로 SQL 탭에서 ClickHouse가 받은 요청을 모두 확인할 수 있습니다. 각 파일 끝의 주석에는 예제가 출력하는 행이 적혀 있습니다.",
    details: {
      "website-analytics":
        "UTC [start, end) 구간에서 한 사이트의 페이지 조회 수와 정확한 고유 방문자 수를 경로별로 집계합니다. 세션 임시 테이블을 사용하며, 동일한 방문자가 여러 경로에서 집계될 수 있습니다.",
      "usage-metering":
        "여러 삽입 배치에 걸친 사용량을 테넌트, 사용자, UTC 분 단위로 집계합니다. CREATE, SELECT, INSERT, DROP 권한이 있는 폐기 가능한 데이터베이스에서 순차적으로 실행하세요. 고정된 이름의 영구 테이블과 구체화된 뷰를 사용하고, 성공적으로 생성한 객체만 정리합니다. 프로세스가 중단되거나 CREATE 응답이 유실되면 객체가 남아 수동으로 확인해야 할 수 있습니다.",
      "request-latency":
        "성공한 요청 수, 가중 평균 지연 시간, 정확한 p95/p99를 테넌트, 작업, UTC 분 단위로 집계합니다. CREATE, SELECT, INSERT, DROP 권한이 있는 폐기 가능한 데이터베이스에서 순차적으로 실행하세요. 성공적으로 생성한 고정된 이름의 테이블과 구체화된 뷰만 정리합니다. 프로세스가 중단되거나 CREATE 응답이 유실되면 객체가 남을 수 있습니다. 정확한 분위수의 집계 상태는 모든 샘플을 보관하므로 프로덕션 워크로드의 메모리 사용량을 평가하세요.",
      "asof-price":
        "ASOF LEFT JOIN은 각 거래를 동일한 심볼의 호가 중 시각이 거래 시각과 같거나 이전인 가장 최근 호가와 연결합니다. Decimal 가격은 문자열로 유지되며, 과거 호가가 없으면 null을 반환합니다. 세션 임시 테이블을 사용합니다.",
      "dynamic-signals":
        "Map(String, Float64)에 담긴 변화하는 모델 신호를 필터링하며, 경계값을 포함하는 임계값을 적용하기 전에 키가 존재하는지 확인합니다. 저장된 0도 유효한 점수로 취급합니다. 세션 임시 테이블을 사용합니다.",
    },
    tabsLabel: "{title}: 소스와 SQL",
  },
} satisfies Strings;
