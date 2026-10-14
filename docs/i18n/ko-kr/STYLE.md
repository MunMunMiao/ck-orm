## ko-KR - 한국어(대한민국)

### 1. 대상과 독자

이 규칙은 대한민국의 표준 한국어(ko-KR)로 작성하는 개발자 문서를 위한 것이다. 독자는 ClickHouse, TypeScript, JavaScript, SQL을 이미 아는 개발자라고 가정한다. 설명은 한국어로 하되 API 이름, 식별자, 키워드, 제품명과 개발자가 실제로 쓰는 기술 용어는 불필요하게 번역하지 않는다. Microsoft의 공식 한국어 스타일 가이드와 국립국어원 자료를 기본 근거로 삼고, React·Vue·TypeScript·MDN·ClickHouse의 한국어 문서에서 실제 개발자 용례를 확인했다. 근거: <https://download.microsoft.com/download/5/b/6/5b62389f-4e3b-4c77-bc18-94efba2fa7bb/kor-kor-StyleGuide.pdf>, <https://ko.react.dev/learn>, <https://ko.vuejs.org/>, <https://www.typescriptlang.org/ko/docs/handbook/2/everyday-types.html>, <https://clickhouse.com/ko/clickhouse>.

`자연스럽다`는 말은 단순히 영어 어순을 한국어 어순으로 바꾼다는 뜻이 아니다. 핵심 동사를 앞세우고, 긴 명사열과 수동 표현을 줄이며, 독자가 바로 실행할 수 있는 문장으로 쓴다. 표기가 확인되지 않아도 평이한 한국어 표현을 정하고, 근거가 부족하면 6절의 `unverified` 상태로 표시한다. 영어를 유지하는 것은 3절의 Concept words 정책에 맞는 기술 용어뿐이다.

### 2. 호칭·문체·어조

- 독자를 `당신`, `귀하`로 부르지 않는다. 한국어 기술 문서에서는 `you`를 생략하고, 꼭 필요할 때만 `사용자`라고 쓴다. 근거: <https://download.microsoft.com/download/5/b/6/5b62389f-4e3b-4c77-bc18-94efba2fa7bb/kor-kor-StyleGuide.pdf>, <https://fontlab.dev/vexy-fontlab-writing-styleguide/fl1992mk/localization/ko/>.
- 설명 문장은 짧은 `~합니다`체로 통일한다. 절차 지시는 `~하세요`로 쓴다. 동료 개발자에게 설명하듯 자연스럽고 구어체에 가까운 문장으로 쓰며, 공문서체·관료체·영어 번역투는 피한다. `해요체`를 기본 문체로 쓰지 않으며, `~하십시오`와 `~하시기 바랍니다`는 이 문서의 공문서형 어조에서 제외한다. Microsoft 가이드와 한국어 개발자 문서의 현재 용례를 함께 참고한다. 근거: <https://download.microsoft.com/download/5/b/6/5b62389f-4e3b-4c77-bc18-94efba2fa7bb/kor-kor-StyleGuide.pdf>, <https://fontlab.dev/vexy-fontlab-writing-styleguide/fl1992mk/localization/ko/>, <https://ko.react.dev/learn>, <https://ko.vuejs.org/>.
- 한 문장에 핵심 동작 하나만 넣는다. 주어가 분명하면 생략하고, 수동태보다 능동태를 쓴다. `~할 수 있습니다`는 실제 기능을 말할 때만 쓰고, 지시문은 `~하면 됩니다` 또는 `~하세요`로 쓴다. `~하는 것이 가능합니다`는 쓰지 않는다. 근거: <https://learn.microsoft.com/ko-kr/style-guide/global-communications/writing-tips>, <https://learn.microsoft.com/ko-kr/style-guide/top-10-tips-style-voice>.
- 문서를 인사말로 열거나 맺지 않는다. 첫 문장은 대상이나 작업을 바로 제시하고, 마지막은 결과·주의 사항·다음 작업 중 하나로 닫는다. 목록에서는 완전한 문장과 구를 섞지 않는다. 짧은 문장과 핵심 우선 원칙은 <https://learn.microsoft.com/ko-kr/style-guide/global-communications/writing-tips>에 따른다.

| English source sentence from `reference.md` | **BAD.** officialese/translationese | GOOD. colloquial developer Korean |
|---|---|---|
| The examples below use a single table so the main flow stays easy to follow. | **BAD.** 아래 예제들은 주된 흐름이 쉽게 따라지는 상태를 유지하기 위하여 단일 테이블을 사용합니다. | 아래 예제는 흐름을 따라가기 쉽도록 테이블 하나만 사용합니다. |
| Builder queries are thenable, so `await query` executes the query directly. | **BAD.** 빌더 쿼리는 thenable이므로 `await query`에 의해 쿼리를 직접 실행하는 것이 가능합니다. | 빌더 쿼리는 thenable이라서 `await query`만 하면 바로 실행됩니다. |
| Use `ckTable(name, columns, options?)` to define a table. | **BAD.** 테이블을 정의하기 위해 `ckTable(name, columns, options?)`을 사용하시기 바랍니다. | 테이블은 `ckTable(name, columns, options?)`로 정의하세요. |
| Avoid forwarding those details directly to untrusted clients. | **BAD.** 해당 세부 정보를 신뢰할 수 없는 클라이언트에게 직접 전달하는 것을 피하시기 바랍니다. | 그 정보는 신뢰할 수 없는 클라이언트에 그대로 보내지 마세요. |

### 3. 피할 표현과 패턴

아래 표현을 문법적으로 모두 틀렸다고 단정하는 것은 아니다. 다만 이 문서의 기본 검사는 번역투와 공문서식 어조를 줄이고, 법적·기술적 의미가 달라질 때만 원문 표현을 남긴다. 짧은 문장, 능동태, 구체적인 동사를 우선하는 원칙은 Microsoft 자료와 한국어 현지화 자료를 참고한다. <https://learn.microsoft.com/ko-kr/style-guide/global-communications/writing-tips> <https://fontlab.dev/vexy-fontlab-writing-styleguide/fl1992mk/localization/ko/>

| Avoid | Why | Say instead |
|---|---|---|
| `~에 의해` | 기본 문체에서는 행위자를 드러내는 능동문을 우선한다. 정확한 행위자 표현이 필요한 문맥까지 금지하지 않는다. 근거: <https://learn.microsoft.com/ko-kr/style-guide/global-communications/writing-tips> | 능동문 또는 `~로`, `~에`, `~에게` |
| `~을/를 통해`를 모든 수단에 반복 | 실제 경로·수단이면 `~로`, 출처면 `~에서` 또는 `~에게서`가 더 분명하다. 이는 이 가이드의 번역투 방지 규칙이다. | `~로`, `~에서`, `~에게서` |
| `~하는 것이 가능합니다` | 명사형을 겹친 장황한 표현이다. 근거: <https://learn.microsoft.com/ko-kr/style-guide/global-communications/writing-tips> | `~할 수 있습니다` 또는 `~하세요` |
| `가지고 있습니다` | 소유가 아닌 존재·구성을 `have`로 옮긴 표현이면 구체적인 동사로 바꾼다. 합법적인 소유 의미까지 금지하지 않는다. 근거: <https://fontlab.dev/vexy-fontlab-writing-styleguide/fl1992mk/localization/ko/> | `있습니다`, `포함합니다`, 구체적인 동사 |
| `~로 인해` | 원인 표현은 문맥에 따라 자연스러운 연결어와 주어를 고른다. 합법적인 인과 표현까지 금지하지 않는다. 이 가이드의 문체 규칙이다. | `~때문에`, `~으로`, 원인을 주어로 둔 문장 |
| `및/또는` | `및`은 둘 다, `또는`은 선택을 뜻하므로 논리를 한 덩어리로 흐리지 않는다. 근거: <https://www.korean.go.kr/front/onlineQna/onlineQnaView.do?mn_id=216&pageIndex=1&qna_seq=317572> | `및` 또는 `또는` |
| `하시기 바랍니다` | 존중 표현이지만 이 개발자 문서에는 지나치게 공식적이다. 근거: <https://fontlab.dev/vexy-fontlab-writing-styleguide/fl1992mk/localization/ko/> | `하세요`, 필요하면 `~하면 됩니다` |
| `~하십시오` | 이 문서의 어조에는 지나치게 공식적인 명령형이다. 근거: <https://download.microsoft.com/download/5/b/6/5b62389f-4e3b-4c77-bc18-94efba2fa7bb/kor-kor-StyleGuide.pdf> | `~하세요` |
| `~되어지다` | 이중 피동이 되어 문장이 무거워진다. 근거: <https://fontlab.dev/vexy-fontlab-writing-styleguide/fl1992mk/localization/ko/> | `~되다`, 행위자를 드러낸 능동문 |
| `~을 수행합니다` | 영어 `perform`을 모든 동사에 붙인 번역투다. 근거: <https://fontlab.dev/vexy-fontlab-writing-styleguide/fl1992mk/localization/ko/> | `실행합니다`, `처리합니다`, `합니다` |
| `~에 대하여` | 기술 설명에는 불필요하게 딱딱한 조사구다. 이 가이드에서는 더 짧은 표현을 우선한다. 근거: <https://learn.microsoft.com/ko-kr/style-guide/word-choice/use-simple-words-concise-sentences> | `~에 대해`, 또는 생략 |
| `~에 있어서` | 영어 `in/with`를 직역한 공문서형 연결어가 되기 쉽다. 이 가이드의 문체 규칙이다. | `~에서`, `~에`, 문장 재구성 |
| `당신은` | 한국어 개발자 문서에서 독자를 직접 부르는 대명사로는 부자연스럽다. 근거: <https://fontlab.dev/vexy-fontlab-writing-styleguide/fl1992mk/localization/ko/> | 주어 생략 또는 `사용자는` |
| `그것`을 반복 | 영어 `it`을 그대로 옮기면 지시 대상이 모호해진다. 근거: <https://fontlab.dev/vexy-fontlab-writing-styleguide/fl1992mk/localization/ko/> | 명사를 반복하거나 생략 |
| `상기`, `본 문서`, `본 항목`, `필히` | 공문서식 지시어가 개발자 문서의 흐름을 끊는다. 근거: <https://fontlab.dev/vexy-fontlab-writing-styleguide/fl1992mk/localization/ko/> | `위`, `이 문서`, `이 항목`, `반드시` |

### 4. 문장 부호·표기·띄어쓰기

- 한글에는 대소문자가 없으므로 한국어 제목은 문장형으로 쓴다. 제품명·API·식별자의 영문 대소문자는 원래 표기를 보존한다. 근거: <https://download.microsoft.com/download/5/b/6/5b62389f-4e3b-4c77-bc18-94efba2fa7bb/kor-kor-StyleGuide.pdf>, <https://learn.microsoft.com/ko-kr/style-guide/top-10-tips-style-voice>.
- 일반 문장의 인용은 큰따옴표 `“ ”`, 인용 안의 인용이나 특정 단어를 부각할 때는 작은따옴표 `‘ ’`를 쓴다. 간접 인용에는 따옴표를 붙이지 않는다. 코드와 인라인 코드의 따옴표는 바꾸지 않는다. 근거: <https://korean.go.kr/front/onlineQna/onlineQnaView.do?mn_id=73&pageIndex=1&qna_seq=332237>.
- 인용부호와 범위 표기의 en 대시를 제외한 Markdown 본문과 HTML의 문장 부호는 반각 ASCII를 사용한다. 전각 일본어식 `（ ）`, `，`, `：`는 쓰지 않는다. 여는 괄호 앞에는 공백을 두지 않는다. 근거: <https://fontlab.dev/vexy-fontlab-writing-styleguide/fl1992mk/localization/ko/>, <https://download.microsoft.com/download/5/b/6/5b62389f-4e3b-4c77-bc18-94efba2fa7bb/kor-kor-StyleGuide.pdf>.
- 조사와 어미는 앞말에 붙인다. 영문·숫자·인라인 코드 뒤에도 공백 없이 붙이고, 실제 한국어 발음의 마지막 소리에 맞춰 조사를 고른다. 끝소리가 모음이면 `를/는`, 자음이면 `을/은`을 쓴다: `API를`, `SQL을`, `JSON은`, `RAM은`, `HTTP로`, `ClickHouse를`, `foo`를, `map`을. 철자가 아니라 발음으로 결정한다. 근거: <https://www.korean.go.kr/front/mcfaq/mcfaqView.do?mcfaq_seq=8320&mn_id=217&pageIndex=4>, <https://www.korean.go.kr/front/onlineQna/onlineQnaView.do?mn_id=27&pageIndex=1&qna_seq=333838>, <https://fontlab.dev/vexy-fontlab-writing-styleguide/fl1992mk/localization/ko/>.
- 라틴 문자 약어와 뒤의 한국어 명사가 별도 단어이면 띄운다: `API 문서`, `SQL 쿼리`, `ClickHouse 서버`. 하나의 식별자·단위·수식어이면 붙인다: `64비트`, `3개`, `25%`. 이 구분은 한국어 문법과 개발자 관용을 함께 따른다. 근거: <https://download.microsoft.com/download/5/b/6/5b62389f-4e3b-4c77-bc18-94efba2fa7bb/kor-kor-StyleGuide.pdf>, <https://fontlab.dev/vexy-fontlab-writing-styleguide/fl1992mk/localization/ko/>.
- 숫자와 한글 단위·분류사는 붙인다(`10초`, `3개`, `64비트`). `ms`, `KiB`, `px` 같은 단위 기호는 프로젝트의 코드 표기와 원문 표기를 보존한다. 일반 산문에서 SI 단위를 띄어 써야 하는지까지는 자료가 일관되지 않아, `unverified`로 두고 같은 페이지 안에서 한 가지 표기를 유지한다. 근거: <https://download.microsoft.com/download/5/b/6/5b62389f-4e3b-4c77-bc18-94efba2fa7bb/kor-kor-StyleGuide.pdf>, <https://www.korean.go.kr/front/onlineQna/onlineQnaView.do?mn_id=90&pageIndex=1&qna_seq=330407>.
- 콜론 앞은 붙이고 뒤는 한 칸 띄운다: `다음과 같습니다: 예`. 시간·비율처럼 한 단위를 이루는 `10:20`, `1:2`는 예외다. 문장이 `~합니다`나 `~하세요`로 끝나면 코드 블록이나 목록 앞에도 콜론 대신 마침표를 쓴다. 근거: <https://www.korean.go.kr/front/onlineQna/onlineQnaView.do?mn_id=27&pageIndex=1&qna_seq=336224>, <https://download.microsoft.com/download/5/b/6/5b62389f-4e3b-4c77-bc18-94efba2fa7bb/kor-kor-StyleGuide.pdf>.
- 완전한 불릿 문장은 마침표로 끝내고, 단어·구만 나열하는 불릿에는 마침표를 붙이지 않는다. 목록 앞에서는 한 문장에 한 가지 구두점 규칙을 적용한다. 근거: <https://learn.microsoft.com/ko-kr/style-guide/global-communications/writing-tips>, <https://download.microsoft.com/download/5/b/6/5b62389f-4e3b-4c77-bc18-94efba2fa7bb/kor-kor-StyleGuide.pdf>.
- 줄임표는 원문에 있는 `...` 또는 `…`의 형태를 유지하고, 말줄임표를 장식처럼 추가하지 않는다. 범위는 en 대시(`–`)를 쓰되 양쪽에 공백을 넣지 않는다. em 대시는 한국어 문장에 억지로 넣지 말고 쉼표·괄호·새 문장으로 풀어 쓴다. 이는 프로젝트의 Markdown 표기 규칙이다.
- 괄호 앞에는 공백을 넣지 않는다: `응답 시간(초)`. 괄호 안의 설명 뒤 조사는 괄호 앞 핵심 단어의 발음에 맞춘다. 근거: <https://download.microsoft.com/download/5/b/6/5b62389f-4e3b-4c77-bc18-94efba2fa7bb/kor-kor-StyleGuide.pdf>.
- 강조는 Markdown의 `**굵게**`, `*기울임*`을 사용하되, 강조할 범위를 한국어 문장 성분에 맞춰 잡는다. 인라인 코드, 링크 대상, 자리 표시자의 글자와 구두점은 바꾸지 않는다. 이 항목은 프로젝트의 공통 문서 결정이다.
- 줄바꿈을 막는 줄바꿈 방지 공백을 일괄 삽입하지 않는다. 제품명·버전·단위가 렌더링에서 분리될 때만 최소 범위에서 사용하고, 소스 코드와 인라인 코드에는 삽입하지 않는다. 일반 한국어 띄어쓰기와 렌더링 보장을 혼동하지 않는다. 이는 프로젝트의 Markdown 규칙이다.

### 5. 숫자·단위·날짜

- 소수점은 마침표, 천 단위 구분은 쉼표를 쓴다: `1,234.56`. 한국어 문서에서도 숫자 표기는 이 형식을 유지한다. 근거: <https://fontlab.dev/vexy-fontlab-writing-styleguide/fl1992mk/localization/ko/>.
- 버전은 원문 그대로 `0.1.0`처럼 쓴다. `0.1.0번째` 같은 서수형은 사용하지 않고 `0.1.0 이전`, `0.1.0 이후`로 쓴다. 버전·식별자 자체는 번역하지 않는다. 근거: <https://download.microsoft.com/download/5/b/6/5b62389f-4e3b-4c77-bc18-94efba2fa7bb/kor-kor-StyleGuide.pdf>.
- `64-bit`는 산문에서 `64비트`로 쓴다. 숫자와 한글 단위 사이의 하이픈은 넣지 않는다. 근거: <https://download.microsoft.com/download/5/b/6/5b62389f-4e3b-4c77-bc18-94efba2fa7bb/kor-kor-StyleGuide.pdf>.
- 백분율은 기술 문서에서 `25%`처럼 붙여 쓴다. `25 퍼센트`도 표준적으로 가능하지만 한 문서에서 두 형식을 섞지 않는다. 근거: <https://www.korean.go.kr/front/onlineQna/onlineQnaView.do?mn_id=216&qna_seq=307529>.
- `for example`은 문장에서는 `예를 들어`, 짧은 괄호나 표에서는 `예:`로 쓴다. `e.g.`를 남기지 않는다. 이 문맥의 약식 표기는 `unverified`지만 한국어 개발 문서 관용에 맞춘 선택이다.
- 로그·API 페이로드·ISO 날짜·타임스탬프·버전은 원문 형식을 보존한다. 설명문 날짜는 `2026년 10월 12일`, 시간은 `15:43` 또는 필요한 경우 `오후 3:43`으로 쓴다. 24시간 시각과 `H:mm:ss`/`HH:mm:ss` 형식은 Microsoft 한국어 가이드에 따른다. 근거: <https://download.microsoft.com/download/5/b/6/5b62389f-4e3b-4c77-bc18-94efba2fa7bb/kor-kor-StyleGuide.pdf>.
- 날짜·시간의 숫자에 불필요한 앞자리 0을 추가하지 않는다. 단, ISO·로그·버전처럼 기계가 읽는 형식은 예외 없이 원문을 보존한다. 근거: <https://download.microsoft.com/download/5/b/6/5b62389f-4e3b-4c77-bc18-94efba2fa7bb/kor-kor-StyleGuide.pdf>.

### 6. 용어

`keep English?`가 `yes`인 행은 Concept words 정책상 SQL 키워드, API 이름, 식별자, 제품명 또는 한국어 개발자가 실제로 영어로 두는 기술 용어다. 그 밖의 개념어는 출처가 부족해도 반드시 구체적인 한국어 표현을 적는다. `unverified`는 영어 유지가 아니라 원어민 확인이 필요한 상태를 뜻한다. 영문명 뒤 조사는 철자가 아니라 실제 한국어 발음에 맞춘다.

| English | local rendering | keep English? | status | source / note |
|---|---|---:|---|---|
| ClickHouse | ClickHouse | yes | attested | <https://clickhouse.com/ko/clickhouse> — 제품명은 원문 표기를 유지한다. 조사 예: ClickHouse를. |
| ck-orm | ck-orm | yes | inferred | 공통 규칙 3절 — 라이브러리명은 음역하지 않는다. 끝소리 발음에 맞춰 ck-orm은, ck-orm을처럼 쓴다. |
| TypeScript | TypeScript | yes | attested | <https://www.typescriptlang.org/ko/docs/handbook/2/everyday-types.html> — 언어명은 원문 표기를 유지한다. |
| JavaScript | JavaScript | yes | attested | <https://developer.mozilla.org/ko/docs/Web/JavaScript> — 언어명은 원문 표기를 유지한다. |
| SQL | SQL | yes | attested | <https://clickhouse.com/ko/clickhouse> — SQL 표준명은 원문 표기를 유지한다. 발음에 따라 SQL을, SQL은처럼 쓴다. |
| JSON | JSON | yes | attested | <https://developer.mozilla.org/ko/docs/Web/API/Request> — 데이터 형식명은 원문 표기를 유지한다. JSON을, JSON은처럼 쓴다. |
| DDL | DDL | yes | inferred | 공통 규칙 3절 — 기술 약어는 원문 표기를 유지한다. |
| NULL | NULL | yes | attested | <https://clickhouse.com/ko/clickhouse> — SQL 키워드이므로 원문을 유지한다. |
| CTE | CTE | yes | inferred | 공통 규칙 3절 — SQL 약어는 원문 표기를 유지한다. |
| HTTP | HTTP | yes | attested | <https://developer.mozilla.org/ko/docs/Web/API/Request> — 프로토콜명은 원문 표기를 유지한다. |
| UTC | UTC | yes | inferred | 공통 규칙 3절 — 시간대 약어는 원문 표기를 유지한다. |
| OpenTelemetry | OpenTelemetry | yes | inferred | <https://opentelemetry.io/ko/docs/concepts/glossary/> 및 공통 규칙 3절 — 프로젝트가 정한 표기를 유지한다. |
| Bun | Bun | yes | inferred | 공통 규칙 3절 — 런타임 제품명은 원문 표기를 유지한다. |
| npm | npm | yes | inferred | 공통 규칙 3절 — 패키지 관리 도구명은 원문 표기를 유지한다. |
| pnpm | pnpm | yes | inferred | 공통 규칙 3절 — 패키지 관리 도구명은 원문 표기를 유지한다. |
| Yarn | Yarn | yes | inferred | 공통 규칙 3절 — 패키지 관리 도구명은 원문 표기를 유지한다. |
| Node.js | Node.js | yes | inferred | 공통 규칙 3절 — 런타임 제품명은 원문 표기를 유지한다. |
| Drizzle | Drizzle | yes | inferred | 공통 규칙 3절 — 라이브러리명은 원문 표기를 유지한다. |
| GitHub | GitHub | yes | inferred | 공통 규칙 3절 — 서비스명은 원문 표기를 유지한다. |
| llms.txt | llms.txt | yes | inferred | 공통 규칙 3절 — 파일명은 원문 표기를 유지한다. |
| MIT | MIT | yes | inferred | 공통 규칙 3절 — 라이선스명은 원문 표기를 유지한다. |
| Decimal | Decimal | yes | inferred | 공통 규칙 3절 — ClickHouse 타입명은 원문 표기를 유지한다. |
| Float64 | Float64 | yes | inferred | 공통 규칙 3절 — ClickHouse 타입명은 원문 표기를 유지한다. |
| API name | API 이름 | no | attested | <https://developer.mozilla.org/ko/docs/Web/API/Request> — API는 보존하고 일반 명사 name은 번역한다. |
| identifier | 식별자 | no | attested | <https://www.typescriptlang.org/ko/docs/handbook/2/everyday-types.html> — 한국어 TypeScript 문서의 용례다. |
| keyword | 키워드 | no | attested | <https://www.typescriptlang.org/ko/docs/handbook/2/everyday-types.html> — 한국어 개발 문서의 관용 표기다. |
| database | 데이터베이스 | no | attested | <https://clickhouse.com/ko/clickhouse> — 일반 개념어이므로 한국어 표기를 쓴다. |
| name | 이름 | no | attested | <https://developer.mozilla.org/ko/docs/Web/API/Request> — name은 이름으로 옮긴다. |
| boundary | 경계 | no | unverified | 출처를 확인하지 못함. 평이한 교과서 표현으로 정했으며 한국어 원어민 확인이 필요하다. |
| with these boundaries in mind | 다음 경계를 염두에 두고 | no | inferred | 한국어 기술 문서의 자연스러운 연결 표현으로 유추했다. |
| by default | 기본적으로 | no | attested | <https://developer.mozilla.org/ko/docs/Web/API/Request> — 한국어 MDN의 기본적으로 용례를 따른다. |
| default path | 기본 경로 | no | inferred | 기본 경로라는 한국어 기술 문장으로 유추했다. |
| automatically | 자동으로 | no | attested | <https://developer.mozilla.org/ko/docs/Web/API/Request> — 한국어 MDN의 자동으로 용례를 따른다. |
| explicitly | 명시적으로 | no | attested | <https://developer.mozilla.org/ko/docs/Web/API/Request> — 한국어 MDN의 명시적으로 용례를 따른다. |
| package | 패키지 | no | attested | <https://www.typescriptlang.org/ko/docs/handbook/modules.html> — 패키지 용례를 따른다. |
| command | 명령 | no | inferred | 한국어 개발자 문서의 명령 용례에서 유추했다. |
| dependency | 종속성 | no | inferred | Microsoft 한국어 개발 문서의 종속성 용례에서 유추했다. |
| ClickHouse-specific | ClickHouse 전용 | no | inferred | 한국어 기술 문서의 전용 표현으로 유추했다. |
| large or high-risk numeric result | 값이 크거나 손실 위험이 큰 숫자 결과 | no | inferred | numeric과 risk를 한국어 개발 문장의 자연스러운 결과 설명으로 풀어 썼다. |
| silently coerce | 암묵적으로 형변환하다 | no | unverified | 출처를 확인하지 못함. 평이한 기술 표현으로 정했으며 한국어 원어민 확인이 필요하다. |
| schema | 스키마 | no | attested | <https://clickhouse.com/ko/clickhouse> — 한국어 ClickHouse 문서의 용례를 따른다. |
| schema DSL | 스키마 DSL | no | inferred | schema는 위 출처를 따르고 DSL은 기술 약어로 보존한다. |
| column | 컬럼 | no | attested | <https://clickhouse.com/ko/clickhouse> — 한국어 ClickHouse 문서의 컬럼 용례를 따른다. |
| table | 테이블 | no | attested | <https://clickhouse.com/ko/clickhouse> — 한국어 ClickHouse 문서의 테이블 용례를 따른다. |
| temporary table | 임시 테이블 | no | attested | <https://learn.microsoft.com/ko-kr/sql/relational-databases/tables/create-tables-database-engine?view=sql-server-ver17> — 임시 테이블 용례를 따른다. |
| table engine | 테이블 엔진 | no | unverified | 출처를 확인하지 못함. 구성 요소를 그대로 풀어 쓴 표현이며 한국어 원어민 확인이 필요하다. |
| column type | 컬럼 타입 | no | inferred | ClickHouse의 컬럼과 TypeScript의 타입 용례를 조합해 정했다. |
| type definition | 타입 정의 | no | attested | <https://www.typescriptlang.org/ko/docs/handbook/2/everyday-types.html> — 타입 정의 용례를 따른다. |
| nullable | NULL 허용 | no | inferred | NULL은 공통 기술명으로 두고 nullable의 동작을 풀어 썼다. |
| optional | 선택적 | no | attested | <https://www.typescriptlang.org/ko/docs/handbook/2/everyday-types.html> — 선택적 용례를 따른다. |
| required | 필수 | no | inferred | TypeScript 문서의 필수 조건 설명에서 유추했다. |
| default value | 기본값 | no | attested | <https://developer.mozilla.org/ko/docs/Web/API/Request> — 기본 값 용례를 따른다. |
| modifier | 수정자 | no | unverified | 출처를 확인하지 못함. 평이한 교과서 표현으로 정했으며 한국어 원어민 확인이 필요하다. |
| materialized column | 구체화된 컬럼 | no | unverified | 출처를 확인하지 못함. materialized의 뜻을 풀어 쓴 표현이며 한국어 원어민 확인이 필요하다. |
| alias column | 별칭 컬럼 | no | unverified | 출처를 확인하지 못함. alias의 표준 개발자 표현을 조합했으며 한국어 원어민 확인이 필요하다. |
| array | 배열 | no | attested | <https://www.typescriptlang.org/ko/docs/handbook/2/everyday-types.html> — 배열 용례를 따른다. |
| tuple | 튜플 | no | attested | <https://www.typescriptlang.org/ko/docs/handbook/2/everyday-types.html> — 튜플 용례를 따른다. |
| map | 맵 | no | attested | <https://www.typescriptlang.org/ko/docs/handbook/iterators-and-generators.html> — Map을 맵으로 설명하는 개발자 용례에서 유추했다. |
| nested | 중첩된 | no | inferred | 한국어 TypeScript 문서의 중첩 구조 표현에서 유추했다. |
| enum | 열거형 | no | attested | <https://www.typescriptlang.org/ko/docs/handbook/enums.html> — 열거형 용례를 따른다. |
| literal | 리터럴 | no | attested | <https://www.typescriptlang.org/ko/docs/handbook/2/everyday-types.html> — 리터럴 용례를 따른다. |
| string | 문자열 | no | attested | <https://www.typescriptlang.org/ko/docs/handbook/2/everyday-types.html> — 문자열 용례를 따른다. |
| integer | 정수 | no | attested | <https://www.typescriptlang.org/ko/docs/handbook/2/everyday-types.html> — 정수 용례를 따른다. |
| 64-bit integer | 64비트 정수 | no | inferred | 한국어 숫자·단위 표기 규칙과 정수 용어를 조합했다. |
| floating point | 부동 소수점 | no | attested | <https://www.typescriptlang.org/ko/docs/handbook/2/everyday-types.html> — 부동 소수점 용례를 따른다. |
| boolean | 불리언 | no | attested | <https://www.typescriptlang.org/ko/docs/handbook/2/everyday-types.html> — 불리언 용례를 따른다. |
| date | 날짜 | no | attested | <https://developer.mozilla.org/ko/docs/Web/API/Request> — 날짜 용례를 따른다. |
| timestamp | 타임스탬프 | no | inferred | 한국어 OpenTelemetry와 웹 개발 문서의 시간 표현에서 유추했다. |
| timezone | 시간대 | no | attested | <https://developer.mozilla.org/ko/docs/Web/API/Request> — 시간대 의미를 풀어 쓴다. |
| precision | 정밀도 | no | attested | <https://clickhouse.com/ko/clickhouse> — 수치 정밀도 용례를 따른다. |
| scale | 스케일 | no | unverified | 출처를 확인하지 못함. 수치 타입의 평이한 기술 표현으로 정했으며 한국어 원어민 확인이 필요하다. |
| exact value | 정확한 값 | no | unverified | 출처를 확인하지 못함. 평이한 교과서 표현으로 정했으며 한국어 원어민 확인이 필요하다. |
| lossless | 무손실 | no | unverified | 출처를 확인하지 못함. 한국어 수치 처리 문맥의 평이한 표현으로 정했으며 한국어 원어민 확인이 필요하다. |
| JSON path | JSON 경로 | no | inferred | JSON은 MDN 한국어 문서 용례를 따르고 path는 경로로 번역한다. |
| generic type | 제네릭 타입 | no | attested | <https://www.typescriptlang.org/ko/docs/handbook/2/generics.html> — 제네릭 타입 용례를 따른다. |
| type inference | 타입 추론 | no | attested | <https://www.typescriptlang.org/ko/docs/handbook/type-inference.html> — 타입 추론 용례를 따른다. |
| inferred | 추론된 | no | attested | <https://www.typescriptlang.org/ko/docs/handbook/type-inference.html> — 추론된 타입 용례를 따른다. |
| typed | 타입이 지정된 | no | inferred | TypeScript 한국어 문서의 타입 표기 설명에서 유추했다. |
| untyped | 타입이 지정되지 않은 | no | inferred | typed의 한국어 대응을 부정형으로 확장했다. |
| type assertion | 타입 단언 | no | attested | <https://www.typescriptlang.org/ko/docs/handbook/2/everyday-types.html> — 타입 단언 용례를 따른다. |
| runtime validation | 런타임 유효성 검사 | no | inferred | 런타임과 유효성 검사라는 한국어 개발자 용례를 조합했다. |
| validator | 검증기 | no | inferred | validation의 한국어 용례와 도구 이름 관행에서 유추했다. |
| encoder | 인코더 | no | inferred | 한국어 개발자 문서의 encoder 음역 관행에서 유추했다. |
| decoder | 디코더 | no | inferred | 한국어 개발자 문서의 decoder 음역 관행에서 유추했다. |
| decoding | 디코딩 | no | inferred | decoder의 동작명으로 유추했다. |
| query | 쿼리 | no | attested | <https://clickhouse.com/ko/clickhouse> — 한국어 ClickHouse 문서의 쿼리 용례를 따른다. |
| query builder | 쿼리 빌더 | no | inferred | query와 builder의 한국어 개발자 용례를 조합했다. |
| builder | 빌더 | no | inferred | 한국어 JavaScript 라이브러리 문서의 빌더 용례에서 유추했다. |
| chain | 메서드 체인 | no | inferred | JavaScript 개발자 문서의 메서드 체인 용례에서 유추했다. |
| chainable | 체이너블 | no | unverified | 출처를 확인하지 못함. 한국어 개발자가 쓰는 음역 후보로 정했으며 원어민 확인이 필요하다. |
| subquery | 서브쿼리 | no | attested | <https://www.kpc.or.kr/PTWED003_dtil_view.do?cono=152959&ecno=47559> — 한국어 SQL 교육 자료의 서브쿼리 용례를 따른다. |
| bare subquery | 이름 없는 서브쿼리 | no | unverified | 출처를 확인하지 못함. bare의 의미를 풀어 쓴 표현이며 한국어 원어민 확인이 필요하다. |
| anonymous CTE | 이름 없는 CTE | no | unverified | 출처를 확인하지 못함. CTE는 기술 약어로 유지하고 anonymous만 풀어 쓴다. |
| join | 조인 | no | attested | <https://learn.microsoft.com/ko-kr/sql/relational-databases/performance/joins?view=sql-server-ver17> — 조인 용례를 따른다. |
| left join | 왼쪽 조인 | no | attested | <https://learn.microsoft.com/ko-kr/sql/relational-databases/performance/joins?view=sql-server-ver17> — LEFT JOIN의 설명형 표현이다. |
| outer join | 외부 조인 | no | attested | <https://learn.microsoft.com/ko-kr/sql/relational-databases/performance/joins?view=sql-server-ver17> — 외부 조인 용례를 따른다. |
| null semantics | NULL 처리 규칙 | no | inferred | SQL NULL과 semantics를 한국어 개발자가 이해하는 동작 설명으로 풀었다. |
| predicate | 조건식 | no | unverified | 출처를 확인하지 못함. SQL 문맥의 평이한 표현으로 정했으며 한국어 원어민 확인이 필요하다. |
| filter | 필터 | no | attested | <https://clickhouse.com/ko/clickhouse> — 필터 용례를 따른다. |
| where clause | WHERE 절 | no | attested | <https://learn.microsoft.com/ko-kr/sql/relational-databases/performance/joins?view=sql-server-ver17> — WHERE는 키워드, clause는 절로 쓴다. |
| group by | GROUP BY 절 | yes | attested | <https://clickhouse.com/ko/clickhouse> — SQL 키워드와 절 이름을 유지한다. |
| order by | ORDER BY 절 | yes | attested | <https://clickhouse.com/ko/clickhouse> — SQL 키워드와 절 이름을 유지한다. |
| limit | LIMIT 절 | yes | attested | <https://clickhouse.com/ko/clickhouse> — SQL 키워드와 절 이름을 유지한다. |
| projection | 프로젝션 | no | unverified | 출처를 확인하지 못함. 한국어 개발자 문서에서 쓰는 음역 후보로 정했으며 원어민 확인이 필요하다. |
| selection (select list) | 선택 목록(SELECT 목록) | no | inferred | selection은 선택 목록으로 풀고 SELECT는 SQL 키워드로 유지한다. |
| select list | SELECT 목록 | no | inferred | SELECT는 SQL 키워드로 유지하고 list는 목록으로 번역한다. |
| alias | 별칭 | no | attested | <https://www.typescriptlang.org/ko/docs/handbook/2/everyday-types.html> — 별칭 용례를 따른다. |
| expression | 표현식 | no | attested | <https://www.typescriptlang.org/ko/docs/handbook/2/everyday-types.html> — 표현식 용례를 따른다. |
| window expression | 윈도 표현식 | no | unverified | 출처를 확인하지 못함. window의 수식어를 풀어 쓴 표현이며 한국어 원어민 확인이 필요하다. |
| aggregate | 집계 | no | attested | <https://clickhouse.com/ko/clickhouse> — 집계 용례를 따른다. |
| function | 함수 | no | attested | <https://www.typescriptlang.org/ko/docs/handbook/2/everyday-types.html> — 함수 용례를 따른다. |
| table function | 테이블 함수 | no | inferred | table과 function의 한국어 SQL 용례를 조합했다. |
| scalar | 스칼라 | no | unverified | 출처를 확인하지 못함. 한국어 개발자 문서에서 쓰는 음역 후보로 정했으며 원어민 확인이 필요하다. |
| raw SQL | 원시 SQL | no | attested | <https://learn.microsoft.com/ko-kr/sql/connect/python/mssql-django/migrate-from-postgresql?view=sql-server-ver17> — raw SQL을 원시 SQL로 옮긴다. |
| SQL template | SQL 템플릿 | no | attested | <https://www.sqlgate.com/assets/brochure/SQLGate_Brochure_KO.pdf> — SQL 템플릿 용례를 따른다. |
| SQL fragment | SQL 조각 | no | inferred | SQL은 유지하고 fragment는 조각으로 번역한다. |
| parameter | 매개변수 | no | attested | <https://developer.mozilla.org/ko/docs/Web/API/Request> — 매개변수 용례를 따른다. |
| bound parameter | 바인딩된 매개변수 | no | inferred | parameter의 한국어 용례와 바인딩이라는 개발자 용례를 조합했다. |
| placeholder | 자리 표시자 | no | attested | <https://learn.microsoft.com/ko-kr/rest/api/storageservices/list-blobs> — 자리 표시자 용례를 따른다. |
| query parameter | 쿼리 매개변수 | no | inferred | query와 parameter의 한국어 용례를 조합했다. |
| compile | 컴파일 | no | attested | <https://www.typescriptlang.org/ko/docs/handbook/2/everyday-types.html> — 컴파일 용례를 따른다. |
| compiled SQL | 컴파일된 SQL | no | inferred | compile의 한국어 동사형과 SQL 표기를 조합했다. |
| thenable | thenable | yes | attested | <https://developer.mozilla.org/ko/docs/Web/JavaScript/Reference/Global_Objects/Promise> — JavaScript 용어로 영어 표기를 유지한다. |
| helper | 헬퍼 | no | attested | <https://ko.react.dev/learn/responding-to-events> — 한국어 React 문서의 헬퍼 용례에서 확인했다. |
| namespace | 네임스페이스 | no | attested | <https://www.typescriptlang.org/ko/docs/handbook/modules.html> — 네임스페이스 용례를 따른다. |
| factory | 팩토리 | no | inferred | 한국어 JavaScript 라이브러리 문서의 팩토리 용례에서 유추했다. |
| callback | 콜백 | no | attested | <https://ko.vuejs.org/guide/essentials/lifecycle> — 콜백 용례를 따른다. |
| iterator | 이터레이터 | no | attested | <https://www.typescriptlang.org/ko/docs/handbook/iterators-and-generators.html> — 이터레이터 용례를 따른다. |
| variadic | 가변 인수 | no | attested | <https://www.typescriptlang.org/ko/docs/handbook/release-notes/typescript-4-0.html> — 가변 인자 튜플 타입 용례를 따른다. |
| insert (SQL keyword) | INSERT | yes | attested | <https://clickhouse.com/ko/clickhouse> — SQL 키워드이므로 원문을 유지한다. |
| insert (write operation) | 삽입 | no | inferred | SQL 키워드가 아닌 동작 설명에서는 한국어 동사 명사를 쓴다. |
| write | 쓰기 | no | inferred | 한국어 데이터베이스 문서의 읽기·쓰기 용례에서 유추했다. |
| batch | 배치 | no | inferred | 한국어 개발자 문서의 배치 처리 용례에서 유추했다. |
| bulk insert | 대량 삽입 | no | inferred | bulk와 insert의 한국어 의미를 조합했다. |
| insert from select | SELECT 결과 삽입 | no | inferred | SELECT는 SQL 키워드로 두고 동작을 한국어로 풀었다. |
| stream | 스트림 | no | attested | <https://developer.mozilla.org/ko/docs/Web/API/Request> — ReadableStream과 스트림 용례를 따른다. |
| streaming | 스트리밍 | no | inferred | stream의 한국어 개발자 용례를 동작명으로 확장했다. |
| row | 행 | no | attested | <https://clickhouse.com/ko/clickhouse> — 데이터베이스 행 용례를 따른다. |
| result set | 결과 집합 | no | attested | <https://learn.microsoft.com/ko-kr/sql/integration-services/integration-services-error-and-message-reference?view=sql-server-ver17> — 결과 집합 용례를 따른다. |
| replacement (ReplacingMergeTree) | 행 교체(ReplacingMergeTree) | no | inferred | replacement는 행 교체로 풀고 ReplacingMergeTree는 엔진명으로 유지한다. |
| ReplacingMergeTree | ReplacingMergeTree | yes | inferred | ClickHouse 엔진 이름이므로 원문을 유지한다. |
| versioning | 버전 관리 | no | inferred | 한국어 소프트웨어 문서의 버전 관리 용례에서 유추했다. |
| deletion flag | 삭제 플래그 | no | inferred | 삭제와 플래그의 개발자 용례를 조합했다. |
| client | 클라이언트 | no | attested | <https://developer.mozilla.org/ko/docs/Web/API/Request> — 클라이언트 용례를 따른다. |
| connection | 연결 | no | attested | <https://developer.mozilla.org/ko/docs/Web/Privacy> — 연결과 전송 설명의 용례를 따른다. |
| server | 서버 | no | attested | <https://developer.mozilla.org/ko/docs/Web/API/Request> — 서버 용례를 따른다. |
| replica | 복제본 | no | inferred | ClickHouse 한국어 문서의 복제 용례에서 유추했다. |
| request | 요청 | no | attested | <https://developer.mozilla.org/ko/docs/Web/API/Request> — 요청 용례를 따른다. |
| response | 응답 | no | attested | <https://developer.mozilla.org/ko/docs/Web/API/Response> — 응답 용례를 따른다. |
| transport | 전송 계층 | no | attested | <https://developer.mozilla.org/ko/docs/Web/Privacy> — 전송 계층 용례를 따른다. |
| HTTP request | HTTP 요청 | no | attested | <https://developer.mozilla.org/ko/docs/Web/API/Request> — HTTP는 유지하고 request는 요청으로 번역한다. |
| timeout | 시간 초과 | no | attested | <https://learn.microsoft.com/ko-kr/dotnet/api/system.net.webrequest.timeout?view=net-10.0> — 시간 초과 용례를 따른다. |
| settings | 설정 | no | attested | <https://learn.microsoft.com/ko-kr/azure/developer/azure-developer-cli/azd-copilot-integration> — 설정 용례를 따른다. |
| configuration | 구성 | no | attested | <https://learn.microsoft.com/ko-kr/azure/developer/azure-developer-cli/azd-copilot-integration> — 구성 용례를 따른다. |
| config | 설정 | no | inferred | configuration의 한국어 대응으로 정하고 코드의 config 이름은 보존한다. |
| option | 옵션 | no | attested | <https://developer.mozilla.org/ko/docs/Web/API/Request> — 옵션과 모드 설명의 용례를 따른다. |
| session | 세션 | no | inferred | 한국어 데이터베이스 개발자 문서의 세션 용례에서 유추했다. |
| temporary-table workflow | 임시 테이블 작업 흐름 | no | unverified | 출처를 확인하지 못함. 각 개념을 평이하게 조합한 표현이며 한국어 원어민 확인이 필요하다. |
| transaction | 트랜잭션 | no | attested | <https://www.kpc.or.kr/PTWED003_dtil_view.do?cclno=49050147&cono=152950&ecno=47560> — 트랜잭션 용례를 따른다. |
| concurrency | 동시성 | no | inferred | 한국어 시스템 문서의 동시성 용례에서 유추했다. |
| lifecycle | 라이프사이클 | no | attested | <https://ko.vuejs.org/guide/essentials/lifecycle> — 라이프사이클 용례를 따른다. |
| cleanup | 정리 | no | inferred | 리소스 정리라는 한국어 개발자 용례에서 유추했다. |
| runtime method | 런타임 메서드 | no | unverified | 출처를 확인하지 못함. runtime과 method의 관용 표현을 조합했으며 한국어 원어민 확인이 필요하다. |
| execute | 실행 | no | attested | <https://developer.mozilla.org/ko/docs/Web/API/Request> — 실행 동작의 용례를 따른다. |
| fetch | 가져오기 | no | attested | <https://developer.mozilla.org/ko/docs/Web/API/Request> — 리소스를 가져오는다는 용례를 따른다. |
| abort | 중단 | no | attested | <https://developer.mozilla.org/ko/docs/Web/API/Request> — AbortSignal과 중단 용례를 따른다. |
| cancel | 취소 | no | inferred | 한국어 API 문서의 취소 동작 용례에서 유추했다. |
| observability | 옵저버빌리티 | no | attested | <https://opentelemetry.io/ko/docs/what-is-opentelemetry/> — 공식 한국어 OpenTelemetry 문서의 용례를 따른다. |
| tracing | 트레이싱 | no | attested | <https://opentelemetry.io/ko/docs/concepts/glossary/> — 트레이싱 용례를 따른다. |
| span | 스팬 | no | attested | <https://opentelemetry.io/ko/docs/concepts/glossary/> — 스팬 용례를 따른다. |
| logging | 로깅 | no | attested | <https://opentelemetry.io/ko/docs/> — 로깅 용례를 따른다. |
| logger | 로거 | no | inferred | logging의 한국어 개발자 용례에서 유추했다. |
| metrics | 메트릭 | no | attested | <https://opentelemetry.io/ko/docs/concepts/signals/> — 메트릭 용례를 따른다. |
| instrumentation | 계측 | no | attested | <https://opentelemetry.io/ko/docs/concepts/glossary/> — 계측 용례를 따른다. |
| hook | 훅 | no | attested | <https://ko.react.dev/learn/responding-to-events> — 이벤트 핸들러와 훅의 한국어 개발자 용례를 따른다. |
| plugin | 플러그인 | no | inferred | 소프트웨어 확장 모듈을 가리키는 한국어 개발자 관용 표기이며, 출처 URL은 열어 확인하지 않았다. |
| event | 이벤트 | no | attested | <https://ko.react.dev/learn/responding-to-events> — 이벤트 용례를 따른다. |
| error | 오류 | no | attested | <https://developer.mozilla.org/ko/docs/Web/API/Request> — 오류 용례를 따른다. |
| exception | 예외 | no | attested | <https://www.typescriptlang.org/ko/docs/handbook/2/everyday-types.html> — 런타임 오류와 예외 용례를 따른다. |
| throws | 예외를 발생시키다 | no | inferred | 한국어 JavaScript 문서의 예외 발생 설명에서 유추했다. |
| rejects | 거부하다 | no | unverified | 출처를 확인하지 못함. Promise 상태를 설명하는 평이한 표현으로 정했으며 한국어 원어민 확인이 필요하다. |
| error model | 오류 모델 | no | inferred | error와 model의 한국어 기술 용례를 조합했다. |
| validation error | 유효성 검사 오류 | no | inferred | validation과 error의 한국어 개발자 용례를 조합했다. |
| stack | 스택 | no | attested | <https://developer.mozilla.org/ko/docs/Web/JavaScript> — 호출 스택 등 개발자 용례를 따른다. |
| retry | 재시도 | no | inferred | 한국어 네트워크 문서의 재시도 용례에서 유추했다. |
| security | 보안 | no | attested | <https://developer.mozilla.org/ko/docs/Web/Security> — 보안 용례를 따른다. |
| SQL injection | SQL 인젝션 | no | attested | <https://developer.mozilla.org/ko/docs/Web/Security> — SQL은 유지하고 injection은 인젝션으로 쓴다. |
| untrusted input | 신뢰할 수 없는 입력 | no | attested | <https://developer.mozilla.org/ko/docs/Web/Security> — 신뢰할 수 없는 입력 용례를 따른다. |
| identifier quoting | 식별자 인용 | no | unverified | 출처를 확인하지 못함. SQL 식별자를 따옴표로 처리한다는 뜻을 풀었으며 한국어 원어민 확인이 필요하다. |
| escaping | 이스케이프 | no | attested | <https://developer.mozilla.org/ko/docs/Web/Security> — 이스케이프 용례를 따른다. |
| credentials | 자격 증명 | no | attested | <https://developer.mozilla.org/ko/docs/Web/API/Request> — Request.credentials의 한국어 용례를 따른다. |
| password | 비밀번호 | no | attested | <https://developer.mozilla.org/ko/docs/Web/Privacy> — 비밀번호 용례를 따른다. |
| allow-list | 허용 목록 | no | unverified | 출처를 확인하지 못함. 보안 정책의 평이한 표현으로 정했으며 한국어 원어민 확인이 필요하다. |
| permission | 권한 | no | attested | <https://developer.mozilla.org/ko/docs/Web/Privacy> — 권한 용례를 따른다. |
| installation | 설치 | no | attested | <https://clickhouse.com/ko/clickhouse> — 설치 용례를 따른다. |
| install | 설치하다 | no | inferred | installation의 자연스러운 동사형으로 정했다. |
| peer dependency | 피어 종속성 | no | unverified | 출처를 확인하지 못함. package-manager 용어를 한국어 개발자식으로 풀었으며 원어민 확인이 필요하다. |
| version | 버전 | no | attested | <https://www.typescriptlang.org/ko/docs/handbook/2/everyday-types.html> — 버전 용례를 따른다. |
| pin a version | 버전을 고정하다 | no | inferred | version과 pin의 한국어 동작 표현을 조합했다. |
| release | 릴리스 | no | inferred | 한국어 소프트웨어 문서의 릴리스 용례에서 유추했다. |
| breaking change | 호환성을 깨는 변경 | no | unverified | 출처를 확인하지 못함. 의미를 정확히 풀어 쓴 표현이며 한국어 원어민 확인이 필요하다. |
| before 1.0 | 1.0 이전 | no | inferred | 한국어 버전 표기 규칙에 맞춰 정했다. |
| pre-release | 프리릴리스 | no | inferred | 한국어 소프트웨어 문서의 프리릴리스 음역 관행에서 유추했다. |
| migration | 마이그레이션 | no | inferred | 한국어 데이터베이스 문서의 마이그레이션 용례에서 유추했다. |
| schema migration | 스키마 마이그레이션 | no | inferred | schema와 migration의 한국어 용례를 조합했다. |
| production | 운영 환경 | no | inferred | 한국어 운영 문서의 운영 환경 용례에서 유추했으며 프로덕션은 필요할 때만 병기한다. |
| example | 예제 | no | attested | <https://ko.react.dev/learn/responding-to-events> — 예제 용례를 따른다. |
| runnable | 실행 가능한 | no | attested | <https://clickhouse.com/ko/clickhouse> — 실행 가능한 예제의 의미로 옮긴다. |
| reference | 레퍼런스 | no | inferred | 한국어 개발 문서에서 참고용 API 문서를 부르는 관용에서 유추했다. |
| documentation | 문서 | no | attested | <https://learn.microsoft.com/ko-kr/style-guide/global-communications/writing-tips> — 문서 용례를 따른다. |
| page | 페이지 | no | attested | <https://developer.mozilla.org/ko/docs/Web/API/Request> — 페이지 용례를 따른다. |
| section | 섹션 | no | attested | <https://ko.react.dev/learn> — 섹션 용례를 따른다. |
| caveat | 주의 사항 | no | unverified | 출처를 확인하지 못함. 기술 문서에서 의미가 분명한 평이한 표현으로 정했으며 한국어 원어민 확인이 필요하다. |
| note | 참고 | no | inferred | 한국어 기술 문서의 참고 표기에서 유추했으며 경고 의미면 주의 사항으로 바꾼다. |
| behavior | 동작 | no | attested | <https://www.typescriptlang.org/ko/docs/handbook/2/everyday-types.html> — 동작 용례를 따른다. |
| semantics | 처리 규칙 | no | unverified | 출처를 확인하지 못함. 의미론보다 독자가 바로 이해할 수 있는 표현으로 정했으며 한국어 원어민 확인이 필요하다. |
| contract | 계약 | no | inferred | API 계약이라는 한국어 개발자 용례에서 유추했다. |
| mode | 모드 | no | attested | <https://developer.mozilla.org/ko/docs/Web/API/Request> — 모드 용례를 따른다. |
| public API | 공개 API | no | inferred | public은 공개로, API는 공통 기술명으로 유지한다. |
| stable | 안정적인 | no | attested | <https://www.typescriptlang.org/ko/docs/handbook/2/everyday-types.html> — 안정성 설명의 한국어 용례에서 확인했다. |
| internal | 내부 | no | inferred | 한국어 소프트웨어 문서의 내부 API 용례에서 유추했다. |
| deprecated | 사용 중단됨 | no | attested | <https://learn.microsoft.com/ko-kr/azure/developer/azure-developer-cli/azd-copilot-integration> — 사용 중단 의미를 한국어로 풀어 쓴다. |
| shape | 형태 | no | unverified | 출처를 확인하지 못함. 객체 형태라는 평이한 표현으로 정했으며 한국어 원어민 확인이 필요하다. |
| surface | API 범위 | no | unverified | 출처를 확인하지 못함. API surface의 뜻을 풀어 쓴 표현이며 한국어 원어민 확인이 필요하다. |
| layer | 계층 | no | attested | <https://developer.mozilla.org/ko/docs/Web/Privacy> — 계층 용례를 따른다. |
| keeps X easy | X를 쉽게 관리할 수 있게 한다 | no | inferred | Microsoft의 간결한 동사 중심 문장 원칙에서 유추했다: <https://learn.microsoft.com/ko-kr/style-guide/word-choice/use-simple-words-concise-sentences>. |
| without hiding | 숨기지 않고 | no | inferred | 한국어 능동문으로 재구성한 표현이다: <https://learn.microsoft.com/ko-kr/style-guide/global-communications/writing-tips>. |
| make everyday access easier to structure | 자주 쓰는 작업을 더 쉽게 구성할 수 있게 한다 | no | inferred | 추상 명사열을 동사 중심으로 바꾸라는 원칙에서 유추했다: <https://learn.microsoft.com/ko-kr/style-guide/word-choice/use-simple-words-concise-sentences>. |
| is rejected | 허용되지 않습니다 | no | inferred | 수동형을 줄이고 결과를 직접 말하는 한국어 문체로 유추했다: <https://learn.microsoft.com/ko-kr/style-guide/global-communications/writing-tips>. |
| is not validated | 검증되지 않습니다 | no | inferred | validation의 한국어 용례와 직접적인 문장 구조에서 유추했다. |
| is decoded conservatively | 보수적으로 디코딩합니다 | no | unverified | 출처를 확인하지 못함. 수치 안전성을 보존하는 평이한 표현으로 정했으며 한국어 원어민 확인이 필요하다. |
| carries | 전달합니다 | no | inferred | 의미가 분명한 직접 동사를 우선하라는 원칙에서 유추했다: <https://learn.microsoft.com/ko-kr/style-guide/global-communications/writing-tips>. |
| exposes | 노출합니다 | no | inferred | API가 값을 외부에 보인다는 뜻을 직접 동사로 풀었다. |
| follows | 따릅니다 | no | inferred | 한국어 기술 문서의 규칙 준수 표현에서 유추했다. |
| stays | 유지됩니다 | no | inferred | 상태 유지의 자연스러운 한국어 표현으로 유추했다. |
| leaves | 남깁니다 | no | inferred | 결과를 남긴다는 의미의 직접 동사로 유추했다. |
| pass | 넘깁니다 | no | inferred | 값을 인자로 전달하는 한국어 개발자 용례에서 유추했다. |
| wrap | 감쌉니다 | no | inferred | 코드와 값을 감싼다는 개발자 용례에서 유추했다. |
| compose | 조합합니다 | no | inferred | 쿼리와 함수를 조합한다는 문맥의 자연스러운 동사로 유추했다. |
| accepts | 받습니다 | no | inferred | 함수 입력을 받는다는 한국어 개발자 용례에서 유추했다. |
| returns | 반환합니다 | no | attested | <https://developer.mozilla.org/ko/docs/Web/API/Request> — 반환합니다 용례를 따른다. |
| throws | 예외를 발생시킵니다 | no | inferred | 한국어 JavaScript 문서의 예외 발생 표현에서 유추했다. |

### 7. 표현 검사

한국어와 다른 지역형의 차이를 단정하는 지역형 혼용 항목은 양쪽 표현을 각각 확인한 자료가 없어 등록하지 않는다. 아래 목록은 지역 차이가 아니라 한국어 개발자 문서의 공문서체·번역투·부자연스러운 표기와 프로젝트 공통 결정에 따른 고유명 표기만 다룬다.

| MUST NOT appear | Why / source | Preferred |
|---|---|---|
| `당신`, `귀하` | 한국어 현지화 문서에서 직접 호칭을 피하고 `사용자` 또는 주어 생략을 권한다. <https://fontlab.dev/vexy-fontlab-writing-styleguide/fl1992mk/localization/ko/> | 생략, `사용자` |
| `상기`, `본 문서`, `본 항목`, `필히` | 공문서식 표현으로 분류되어 더 쉬운 `위`, `이 문서`, `이 항목`, `반드시`가 권장된다. <https://fontlab.dev/vexy-fontlab-writing-styleguide/fl1992mk/localization/ko/> | `위`, `이 문서`, `이 항목`, `반드시` |
| `메세지`, `컨텐츠`, `디렉토리`, `쉐이프`, `브러쉬` | 한국어 제품 현지화 가이드가 권장 표기를 각각 `메시지`, `콘텐츠`, `디렉터리`, `셰이프`, `브러시`로 제시한다. <https://fontlab.dev/vexy-fontlab-writing-styleguide/fl1992mk/localization/ko/> | `메시지`, `콘텐츠`, `디렉터리`, `셰이프`, `브러시` |
| `리모콘` | 국립국어원은 관용 표기 `리모컨`을 제시한다. <https://www.korean.go.kr/front/onlineQna/onlineQnaView.do?mn_id=216&qna_seq=306279> | `리모컨` |
| `클릭하우스` | ClickHouse는 프로젝트가 정한 제품명 표기이므로 음역하지 않는다. 공통 규칙 3절. | `ClickHouse` |
| `씨케이오알엠` | ck-orm은 프로젝트가 정한 라이브러리명이므로 음역하지 않는다. 공통 규칙 3절. | `ck-orm` |
| `타입스크립트` | TypeScript는 프로젝트가 정한 언어명이므로 음역하지 않는다. 공통 규칙 3절. | `TypeScript` |
| `자바스크립트` | JavaScript는 프로젝트가 정한 언어명이므로 음역하지 않는다. 공통 규칙 3절. | `JavaScript` |
| `오픈텔레메트리` | OpenTelemetry는 프로젝트가 정한 제품·표준명이므로 음역하지 않는다. 공통 규칙 3절. | `OpenTelemetry` |
| `드리즐` | Drizzle은 프로젝트가 정한 라이브러리명이므로 음역하지 않는다. 공통 규칙 3절. | `Drizzle` |
| `하시기 바랍니다`, `~하십시오` | 이 문서의 개발자용 어조에는 지나치게 공식적인 명령형이다. 한국어 가이드의 친근한 지시형을 따른다. <https://download.microsoft.com/download/5/b/6/5b62389f-4e3b-4c77-bc18-94efba2fa7bb/kor-kor-StyleGuide.pdf> | `~하세요` |
| `~되어지다` | 이중 피동으로 분류되어 `~되다` 또는 능동문으로 고친다. <https://fontlab.dev/vexy-fontlab-writing-styleguide/fl1992mk/localization/ko/> | `~되다` |
| `~로 인해` | 이 가이드에서는 문맥에 맞는 짧은 원인 표현을 우선한다. 합법적인 인과 표현까지 금지하지 않는 WARN 항목이다. <https://learn.microsoft.com/ko-kr/style-guide/word-choice/use-simple-words-concise-sentences> | `~때문에`, `~으로` |

### 8. 모델 문장

다음 경계를 염두에 두고 `ck-orm`을 사용하세요:

- `ckType.*`는 스키마의 컬럼 타입을 정의합니다.
- `ckTable(...)`은 테이블 스키마를 정의합니다.
- `ck.*`는 쿼리 헬퍼 네임스페이스입니다.
- `fn.*`는 SQL 함수 헬퍼 네임스페이스입니다.
- 스키마는 데이터베이스 이름이 아니라 테이블과 컬럼을 설명합니다.
- `clickhouseClient(...)`에 데이터베이스 연결을 설정합니다.
- 빌더 쿼리를 기본 경로로 사용하고, ClickHouse 전용 표현식이 필요하면 원시 SQL을 사용할 수 있습니다.
- `runInSession()`은 ClickHouse 세션 헬퍼이며 트랜잭션이 아닙니다.
- `leftJoin()`은 기본적으로 SQL 방식의 NULL 처리 규칙을 사용합니다.
- 값이 크거나 손실 위험이 큰 숫자 결과는 암묵적으로 형변환하지 않고 보수적으로 디코딩합니다.

ck-orm에는 필수 종속성이 없습니다. `@opentelemetry/api`는 `ck-orm/otel` 플러그인에서만 사용하는 선택적 피어 종속성이므로, 이 플러그인을 사용할 때 ck-orm과 함께 설치하세요.

### 9. 참고한 자료

- <https://download.microsoft.com/download/5/b/6/5b62389f-4e3b-4c77-bc18-94efba2fa7bb/kor-kor-StyleGuide.pdf> — Microsoft Korean Localization Style Guide의 문체, 날짜·시간, 조사, 영문·숫자 띄어쓰기, 피동·명사열·대명사·차용어 지침.
- <https://learn.microsoft.com/en-us/globalization/reference/microsoft-style-guides> — Microsoft 언어별 스타일 가이드와 한국어 가이드 연결.
- <https://learn.microsoft.com/ko-kr/globalization/reference/microsoft-language-resources> — Microsoft 용어 및 현지화 리소스의 일관성 원칙.
- <https://learn.microsoft.com/ko-kr/style-guide/global-communications/writing-tips> — 짧은 문장, 능동태, 목록, 일관된 용어.
- <https://learn.microsoft.com/ko-kr/style-guide/word-choice/use-simple-words-concise-sentences> — 쉬운 단어와 직접적인 문장.
- <https://learn.microsoft.com/ko-kr/style-guide/top-10-tips-style-voice> — 말하듯 쓰기, 핵심 먼저 쓰기, 간결한 문장.
- <https://www.korean.go.kr/front/etcData/etcDataView.do?etc_seq=86&mn_id=46&pageIndex=21> — 국립국어원 외래어 표기법 자료.
- <https://www.korean.go.kr/front/mcfaq/mcfaqView.do?mcfaq_seq=8320&mn_id=217&pageIndex=4> — 영문 표기 뒤 조사 선택을 실제 발음으로 결정하는 원칙.
- <https://www.korean.go.kr/front/onlineQna/onlineQnaView.do?mn_id=27&pageIndex=1&qna_seq=333838> — 조사는 앞말에 붙이는 원칙.
- <https://www.korean.go.kr/front/onlineQna/onlineQnaView.do?mn_id=27&pageIndex=1&qna_seq=336224> — 콜론 앞뒤 띄어쓰기와 시간·비율 예외.
- <https://www.korean.go.kr/front/onlineQna/onlineQnaView.do?mn_id=216&pageIndex=1&qna_seq=317572> — `및`과 `또는`의 의미 차이.
- <https://ko.react.dev/learn/responding-to-events> — 한국어 React 문서의 이벤트, 헬퍼, 합니다체 용례.
- <https://ko.vuejs.org/guide/essentials/lifecycle> — 라이프사이클, 콜백, 렌더링, 마운트 용례.
- <https://www.typescriptlang.org/ko/docs/handbook/2/everyday-types.html> — 타입 추론, 타입 단언, 타입 관련 용례.
- <https://www.typescriptlang.org/ko/docs/handbook/type-inference.html> — 타입 추론과 매개변수 용례.
- <https://www.typescriptlang.org/ko/docs/handbook/2/generics.html> — 제네릭과 타입 인수 용례.
- <https://developer.mozilla.org/ko/docs/Web/API/Request> — 요청, 매개변수, 프로미스, 설정, 자격 증명 용례.
- <https://developer.mozilla.org/ko/docs/Web/API/Response> — 응답 용례.
- <https://developer.mozilla.org/ko/docs/Web/Security> — 보안, 입력, 이스케이프 용례.
- <https://learn.microsoft.com/ko-kr/sql/relational-databases/performance/joins?view=sql-server-ver17> — 조인과 SQL 절 용례.
- <https://learn.microsoft.com/ko-kr/sql/connect/python/mssql-django/migrate-from-postgresql?view=sql-server-ver17> — 원시 SQL 용례.
- <https://opentelemetry.io/ko/docs/what-is-opentelemetry/> — 옵저버빌리티, 트레이싱, 메트릭, 계측 용례.
- <https://opentelemetry.io/ko/docs/concepts/glossary/> — 스팬, 계측, 요청, 오류 관련 용례.
- <https://opentelemetry.io/ko/docs/concepts/signals/> — 트레이스, 메트릭, 로그 용례.
- <https://clickhouse.com/ko/clickhouse> — 스키마, 컬럼, 테이블, 쿼리, 필터, 집계 용례.
- <https://fontlab.dev/vexy-fontlab-writing-styleguide/fl1992mk/localization/ko/> — 한국어 제품 현지화의 대명사, 조사, 단위, 흔한 오탈자·번역투에 대한 2차 자료.
- <https://www.korean.go.kr/front/onlineQna/onlineQnaView.do?mn_id=216&qna_seq=306279> — 리모컨 관용 표기.

### 10. 남은 쟁점

- `체이너블`은 한국어 JavaScript 문서에서 확인할 수 있는 직접 출처를 찾지 못해 음역으로 정했다. 한국어 원어민은 `체인형` 또는 다른 표현이 더 자연스러운지 확인해야 한다.
- `프로젝션`과 `윈도 표현식`은 SQL 문맥의 한국어 팀별 용례가 갈릴 수 있다. `투영`, `윈도우 표현식` 같은 대안과 비교해 확인해야 한다.
- `피어 종속성`은 package-manager 문서에서 한국어 표준화 정도가 낮다. 대상 독자가 `피어 의존성`을 더 자연스럽게 느끼는지 확인해야 한다.
- `운영 환경`은 `production`을 설명문에서 자연스럽게 풀어 쓴 선택이다. 팀이 `프로덕션`을 기본 용어로 쓰는지 확인해야 한다.
- `API 범위`와 `구체화된 컬럼`은 각각 `surface`와 `materialized column`을 뜻풀이한 표현이다. 실제 한국 개발자 문서의 선호어를 원어민이 확인해야 한다.
- 위 항목 외에도 제6절에서 `unverified`로 표시한 모든 행은 원어민 확인 전의 임시 표현이다.


