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
      "각 예제는 {folder}에 있는 실행 가능한 프로젝트입니다. 프로젝트에는 {main}과 {package}이 있습니다. 이 페이지는 그 {main} 파일을 그대로 보여 줍니다. SQL 탭에는 이 파일이 ClickHouse로 보내는 요청이 모두 나오며, 예제의 단위 테스트가 이 SQL을 검증합니다. 각 파일 끝의 주석에는 예제가 출력하는 행이 적혀 있습니다.",
    runIntro: "각 예제는 자체 세션용 임시 테이블을 만들므로 어떤 ClickHouse 서버에서도 실행할 수 있습니다:",
    tabsLabel: "{title}: 소스와 SQL",
  },
} satisfies Strings;
