## es-ES - Español (España)

### 1. Variant and reader

Esta guía es para desarrolladores de España: español europeo (es-ES), no
español latinoamericano ni «español neutro». Microsoft mantiene guías separadas
para España y México; la localización debe elegir una variante y no mezclarlas.
[Índice de guías de estilo de Microsoft](https://learn.microsoft.com/ca-es/globalization/reference/microsoft-style-guides)

El lector usa un ordenador, abre un archivo y ejecuta comandos. Háblale de tú y
usa formas naturales para un desarrollador español: ordenador, archivo (fichero
también es válido según el contexto), contraseña y consulta. FundéuRAE confirma
que «computadora» es correcta y que «ordenador» es la forma usada en España;
esta guía elige «ordenador» por variante, no porque «computadora» sea incorrecta.
[FundéuRAE, computadora/ordenador](https://www.fundeu.es/consulta/computadoraordenador-1215/)

Usa «vosotros» cuando haya que dirigirse informalmente a varias personas; en el
resto, «tú». «Ustedes» también es una forma válida y aparece en algunas zonas
de España, así que aquí es una decisión de casa, no una prohibición gramatical.
[FundéuRAE, Tú, vos, usted](https://www.fundeu.es/consulta/tu-vos-usted-49/)

| Concepto | es-ES | América Latina | Evidencia y decisión |
|---|---|---|---|
| computer | ordenador | computadora | FundéuRAE confirma ambas formas y señala el uso de «ordenador» en España; esta guía elige «ordenador». [Fuente](https://www.fundeu.es/consulta/computadoraordenador-1215/) |
| informal plural you | vosotros / os | ustedes / les | Fundéu describe la distribución y también documenta «ustedes» en zonas de España; esta guía prefiere «vosotros». [Fuente](https://www.fundeu.es/consulta/tu-vos-usted-49/) |
| file | archivo; fichero también es natural | archivo | No se ha verificado una oposición regional estricta; la documentación española de Microsoft usa ambos. Se prefiere «archivo». [Microsoft](https://learn.microsoft.com/es-es/windows/win32/shell/context) |

| array | `array` (forma habitual no fijada) | arreglo | No hay aquí una fuente suficiente para una diferencia regional; React en español usa «arreglo», pero esta guía conserva `array` como forma de equipo y lo deja para revisión nativa. [React](https://es.react.dev/learn) |

### 2. Address, register and voice

Usa «tú», imperativos directos y verbos activos: «comprueba», «pasa»,
«ejecuta», «puedes». «Puedes...» expresa una capacidad u opción; no lo
conviertas en «se podrá...» o «el usuario podrá...». Microsoft recomienda la
segunda persona para un tono humano y la voz activa para mensajes claros.
[Person](https://learn.microsoft.com/es-es/style-guide/grammar/person),
[writing tips](https://learn.microsoft.com/es-es/style-guide/global-communications/writing-tips),
[error-message guidelines](https://learn.microsoft.com/es-es/windows/win32/debug/error-message-guidelines)

Prefiere frases cortas, un concepto por frase y una instrucción por línea. Abre
una sección con el resultado que necesita el lector y ciérrala con la
consecuencia práctica o el siguiente paso. En procedimientos, el imperativo es
natural; en explicaciones, «puedes...». No uses saludos ni despedidas en una
referencia técnica. [Microsoft, writing tips](https://learn.microsoft.com/es-es/style-guide/global-communications/writing-tips)

| English source from `reference.md` | BAD (officialese/translationese) | Good (colloquial es-ES) |
|---|---|---|
| `Use \`ck-orm\` with these boundaries in mind:` | **BAD:** Utilice `ck-orm` teniendo en cuenta los límites que se indican a continuación. | Ten en cuenta estos límites al usar `ck-orm`: |
| `The examples below use a single table so the main flow stays easy to follow.` | **BAD:** Los ejemplos siguientes utilizan una única tabla con el fin de que el flujo principal permanezca fácil de seguir. | Los ejemplos siguientes usan una sola tabla para que sea fácil seguir el recorrido principal. |
| `ClickHouse does not provide traditional transactions.` | **BAD:** No se proporcionan transacciones tradicionales por parte de ClickHouse. | ClickHouse no ofrece transacciones tradicionales. |
| `Avoid forwarding those details directly to untrusted clients.` | **BAD:** Se procederá a evitar el reenvío directo de dichos detalles a clientes no confiables. | No reenvíes esos detalles directamente a clientes que no sean de confianza. |

### 3. Words and patterns to avoid

| Avoid | Why | Say instead |
|---|---|---|
| `se procederá a` | Perífrasis administrativa y agente oculto. | `haz`, `ejecuta`, `se hará` |
| `el usuario deberá` | Tercera persona legalista cuando se habla con el lector. | `tienes que`, `debes` o imperativo |
| `se puede` | Oculta quién debe actuar cuando sí importa. | `puedes` |
| `en el caso de que` | Calco largo. | `si` |
| `con el fin de` / `a fin de` | Nominalización innecesaria. | `para` |
| `llevar a cabo` | Perífrasis sin precisión adicional. | `hacer`, `ejecutar`, `completar` |
| `hacer uso de` | Calco administrativo. | `usar` |
| `proceder a` | Anuncia una acción en vez de expresarla. | `abre`, `ejecuta`, `haz` |
| `dicho/dicha/el mismo/la misma` como pronombres | Suena jurídico y puede ocultar el referente. | `ese/esa` o repite el sustantivo |
| `en relación con` / `a efectos de` | Relleno abstracto o administrativo. | `sobre` / `para` |
| `de cara a` | Jerga corporativa. | `para` / `antes de` |
| `el cual/la cual` repetido | Es correcto, pero RAE señala el uso excesivo como problema de estilo. | `que` o una frase nueva |
| Gerundio de posterioridad o conclusión, por ejemplo `, devolviendo...` | Presenta una consecuencia como simultánea. | Separa la acción: `... y devuelve...` |
| `es validado` / `es enviado` con agente conocido | Pasiva perifrástica innecesaria. | `valida` / `envía`; `se valida` si el agente no importa |
| `utilizar` cuando basta `usar` | Más solemne sin más precisión. | `usar` |
| `en base a` | Muletilla administrativa; esta guía prefiere una relación explícita. | `según` / `a partir de` |

Microsoft recomienda palabras sencillas, frases concisas y voz activa. RAE
documenta el gerundio de conclusión/posterioridad; la advertencia sobre el
exceso de `el cual` es una decisión editorial de esta guía. Son reglas de
claridad, no una prohibición de toda pasiva, gerundio o relativo. [Microsoft, simple
words](https://learn.microsoft.com/es-es/style-guide/word-choice/use-simple-words-concise-sentences),
[RAE, gerundio de conclusión](https://www.rae.es/libro-estilo-justicia/las-palabras-y-sus-grupos-problemas-y-actuaciones/gerundio/usos-incorrectos/gerundio-de-conclusión)

### 4. Punctuation, typography and spacing

- Usa puntuación de ancho normal: `, . ; : ( ) [ ] { }`. No
  introduzcas formas de ancho completo. Entre palabras, dígitos y código inline
  usa un espacio normal cuando la sintaxis lo pida; no pongas espacios dentro
  de los backticks ni alteres el código. Nombres de paquetes, comandos, tablas y
  columnas conservan su forma literal. [Microsoft, text formatting](https://learn.microsoft.com/es-es/contribute/content/text-formatting-guidelines),
  [punctuation and separators](https://learn.microsoft.com/es-es/globalization/internationalization/punctuation-separators)
- En prosa, usa comillas angulares `«...»`; dentro, inglesas curvas
  `“...”`; en un tercer nivel, simples curvas `‘...’`. No
  dejes espacios entre las comillas y su contenido. Jerarquía: angulares,
  inglesas, simples. [FundéuRAE, comillas](https://www.fundeu.es/recomendacion/comillas-uso-de-este-signo-ortografico/)
- En preguntas y exclamaciones directas usa apertura y cierre:
  `¿Puedes ejecutar la consulta?`, `¡Cuidado!`. [FundéuRAE](https://www.fundeu.es/recomendacion/interrogacion-y-exclamacion-usos-de-los-signos-ortograficos/),
  [Microsoft, punctuation](https://learn.microsoft.com/es-es/globalization/internationalization/punctuation-separators)
- Usa `…` para una elipsis real, sin espacios entre puntos; en
  documentación técnica suele ser mejor terminar la frase. [RAE, puntos
  suspensivos](https://www.rae.es/libro-estilo-justicia/signos-de-puntuación/los-puntos-suspensivos/principales-usos-de-los-puntos-suspensivos)
- Usa la raya `—` para un inciso sin espacios, el guion `-`
  para palabras o identificadores y la semirraya `–` para intervalos.
  El signo menos matemático es `−`. [Microsoft, dashes and hyphens](https://learn.microsoft.com/es-es/style-guide/punctuation/dashes-hyphens/)
- Los dos puntos van pegados a la palabra anterior y llevan un espacio después.
  Antes de una lista o bloque de código, termina la introducción con
  `:` y empieza el bloque en la línea siguiente. Tras los dos puntos,
  minúscula en listas o explicaciones; mayúscula si comienza una cita u oración
  independiente. [RAE, dos puntos](https://www.rae.es/ortografía/los-dos-puntos)
- En listas de fragmentos, minúscula y sin punto final; en listas de oraciones
  completas, mayúscula y punto final en cada elemento. [Microsoft, writing tips](https://learn.microsoft.com/es-es/style-guide/global-communications/writing-tips)
- Los encabezados usan sentence case: mayúscula solo en la primera palabra y
  nombres propios; no Title Case ni TODO EN MAYÚSCULAS. Nombres de API y
  productos conservan su forma. [Microsoft, sentence capitalization](https://learn.microsoft.com/es-es/style-guide/text-formatting/using-type/use-sentence-style-capitalization),
  [Microsoft, capitalization checklist](https://learn.microsoft.com/es-es/style-guide/checklists/capitalization-checklist)
- Usa cursiva/negrita solo para énfasis funcional; usa backticks para código,
  no comillas. [Microsoft, text formatting](https://learn.microsoft.com/es-es/contribute/content/text-formatting-guidelines)
- Los paréntesis no llevan espacios interiores y aíslan información
  complementaria; si el inciso ya tiene puntuación, son preferibles a dos
  comas. [RAE, paréntesis](https://www.rae.es/ortografía/los-paréntesis-como-signo-delimitador)
- Usa espacios no separables: `25 %`, `30 s`,
  `128 KiB` y `30 000`. Evitan saltos entre número y símbolo.
  [FundéuRAE, porcentaje](https://www.fundeu.es/consulta/grafia-del-simbolo-del-porcentaje-2742/),
  [FundéuRAE, espacio con símbolos](https://www.fundeu.es/recomendacion/el-se-escribe-separado-de-la-cifra-a-la-que-acompana-802/)

### 5. Numbers, units and dates

- En prosa es-ES usa coma decimal: `3,14`. RAE acepta coma y punto,
  pero la coma es la opción local; Microsoft muestra coma en ejemplos es-ES. En
  código, SQL, JSON, versiones y API conserva el formato literal:
  `0.1.0`, `1.25`, `Decimal(10, 2)`. [FundéuRAE,
  decimales](https://www.fundeu.es/recomendacion/decimales-coma-y-punto-son-ambas-validas/),
  [Microsoft Power Fx](https://learn.microsoft.com/es-es/power-platform/power-fx/reference/function-value)
- Para millares, no uses punto ni coma: `8 327 451`. En cuatro cifras,
  `1000`; conserva cualquier separador de un literal. [FundéuRAE,
  millares](https://www.fundeu.es/consulta/miles-6498/),
  [puntos y comas](https://www.fundeu.es/consulta/puntos-y-comas-en-cifras-739/)
- Separa número y unidad: `500 ms`, `64 bits`,
  `25 %`. Los símbolos no llevan punto ni plural. [FundéuRAE,
  espacio con símbolos](https://www.fundeu.es/recomendacion/el-se-escribe-separado-de-la-cifra-a-la-que-acompana-802/)
- Evita ordinales abreviados; si hacen falta, usa `1.º`,
  `2.ª`, `3.er`. [RAE, formación](https://www.rae.es/ortografía/formación)
- Las versiones son identificadores, no decimales: `0.1.0`,
  `1.0.0`, `v2`. Escribe «entero de 64 bits» en prosa y
  conserva `64-bit` dentro de código o nombres. [Microsoft, text formatting](https://learn.microsoft.com/es-es/contribute/content/text-formatting-guidelines)
- Usa `p. ej.` para «por ejemplo» y `etc.` para «etcétera»;
  no mezcles `e.g.` con prosa española ni escribas `etc...`.
  Fundéu confirma la grafía `p. ej.` con puntos y espacios. [FundéuRAE](https://www.fundeu.es/consulta/por-ejemplo-1687/)
- Fechas: `12 de octubre de 2026`; horas: `14:30`; meses
  en minúscula. En datos y formatos intercambiados conserva ISO:
  `2026-10-12T14:30:00Z`. [Microsoft, formatos globales](https://learn.microsoft.com/es-es/windows/apps/design/globalizing/use-global-ready-formats),
  [Microsoft, fechas y números](https://learn.microsoft.com/es-es/ai-builder/extract-dates-and-numbers)

### 6. Terminology

La columna keep English? indica si el texto debe conservar literalmente el
término inglés. Solo se responde «sí» cuando se trata de un nombre propio,
identificador, palabra clave de SQL o término de la plataforma que la
documentación técnica en español deja en inglés. «Unverified» no autoriza a
dejar el término inglés: en ese caso se usa la forma española indicada y se
deja constancia de que falta confirmación nativa. Las formas de código y los
nombres de API se conservan aunque la explicación use la forma española.
Cuando una fila agrupa conceptos relacionados, las formas de la columna local
siguen exactamente el mismo orden que las de la columna English.

| English | local rendering | keep English? | status | source / note |
|---|---|---|---|---|
| ClickHouse; ck-orm; TypeScript; JavaScript; SQL; JSON; Decimal; DDL; NULL; CTE; HTTP; UTC; OpenTelemetry; Bun; npm; pnpm; Yarn; Node.js; Drizzle; GitHub; llms.txt; MIT | Se conservan sin traducir | sí | attested — https://learn.microsoft.com/es-es/contribute/content/text-formatting-guidelines | Nombres de producto, lenguajes, formatos, acrónimos, runtimes, herramientas, archivos y licencia. |
| API names | nombres de API | sí | attested — https://learn.microsoft.com/es-es/contribute/content/text-formatting-guidelines | API se conserva; los nombres concretos no se traducen. |
| identifiers; keywords | identificadores; palabras clave | no | attested — https://learn.microsoft.com/es-es/contribute/content/text-formatting-guidelines | Se traducen los conceptos; el identificador o la palabra clave literal se conserva. |
| schema; schema DSL | esquema; DSL de esquemas | no | attested — https://es.vuejs.org/v2/guide/ | DSL es el acrónimo; «esquema» es masculino. |
| column; table; temporary table; table engine | columna; tabla; tabla temporal; motor de tablas | no | attested — https://learn.microsoft.com/es-es/fabric/database/sql/query | Cada forma concuerda con su sustantivo; el nombre concreto del motor permanece literal. |
| column type; type definition | tipo de columna; definición de tipo | no | attested — https://www.typescriptlang.org/es/play/javascript/functions-with-javascript/generic-functions.ts.html | «Tipo» es masculino y «definición» femenino. |
| nullable; optional; required; default value | que admite NULL; opcional; obligatorio; valor predeterminado | no | attested — https://learn.microsoft.com/es-es/fabric/database/sql/query | Nullable se explica como «que admite NULL»; los adjetivos concuerdan con el sustantivo. |
| modifier; materialized column; alias column | modificador; columna materializada; columna de alias | no | attested — https://learn.microsoft.com/es-es/contribute/content/text-formatting-guidelines | «Materializada» concuerda con «columna»; ALIAS y MATERIALIZED solo quedan en código. |
| array; tuple; map; nested; enum | matriz; tupla; mapa; anidado; enumeración | no | inferred — https://developer.mozilla.org/es/docs/Glossary/Array | MDN usa «arreglo» y «matriz»; se elige «matriz» para es-ES. Array, Map y enum solo se conservan como nombres de API o identificadores. |
| literal; string; integer; 64-bit integer; floating point; boolean | literal; cadena; entero; entero de 64 bits; coma flotante; booleano | no | attested — https://developer.mozilla.org/es/docs/Web/JavaScript/Guide/Grammar_and_types | «Cadena» es la forma de MDN; booleano/booleana y los demás adjetivos concuerdan con el sustantivo. |
| date; timestamp; timezone | fecha; marca de tiempo; zona horaria | no | attested — https://learn.microsoft.com/es-es/windows/apps/design/globalizing/use-global-ready-formats | «Marca» y «zona» son femeninos; no dejar timestamp en prosa. |
| precision; scale; exact value; lossless | precisión; escala; valor exacto; sin pérdida | no | attested — https://learn.microsoft.com/es-es/fabric/database/sql/query | «Precisión» y «escala» son femeninos; «sin pérdida» es una locución invariable. |
| JSON path | ruta JSON | no | attested — https://developer.mozilla.org/es/docs/Web/JavaScript/Guide/Grammar_and_types | Se traduce path; JSON permanece como nombre de formato. |
| generic type; type inference; inferred | tipo genérico; inferencia de tipos; inferido | no | attested — https://es.vuejs.org/v2/guide/typescript | Genérico e inferido concuerdan con el sustantivo; «inferencia» es femenino. |
| typed; untyped; type assertion | tipado; sin tipar; aserción de tipos | no | attested — https://es.vuejs.org/v2/guide/typescript | Tipado/tipada según el sustantivo; la aserción es la operación explícita. |
| runtime validation; validator | validación en tiempo de ejecución; validador | no | attested — https://learn.microsoft.com/es-es/windows/win32/debug/error-message-guidelines | No usar runtime en prosa; validador/validadora concuerda con el referente. |
| encoder; decoder; decoding | codificador; decodificador; decodificación | no | attested — https://developer.mozilla.org/es/docs/Web/JavaScript/Guide/Grammar_and_types | Se usa «decodificación» de forma coherente. |
| query; query builder; builder | consulta; constructor de consultas; constructor | no | attested — https://learn.microsoft.com/es-es/dotnet/csharp/programming-guide/classes-and-structs/using-constructors | En prosa, builder se traduce; un nombre de clase o API se conserva literalmente. |
| chain; chainable; subquery; bare subquery; anonymous CTE | encadenamiento; encadenable; subconsulta; subconsulta sin alias; CTE anónimo | no | attested — https://developer.mozilla.org/es/docs/Web/JavaScript/Reference/Global_Objects/Promise/then | «Sin alias» expresa que no se ha aplicado .as(); CTE permanece como acrónimo. |
| join; left join; outer join | unión; unión izquierda; unión externa | sí | attested — https://learn.microsoft.com/es-es/fabric/database/sql/query | En explicación se usa la forma española; JOIN, LEFT JOIN y OUTER JOIN se conservan como palabras clave/API. |
| null semantics; predicate; filter | semántica de NULL; predicado; filtro | no | attested — https://learn.microsoft.com/es-es/fabric/database/sql/query | NULL permanece como palabra clave; los tres conceptos se traducen en la explicación. |
| where clause; group by; order by; limit | cláusula WHERE; agrupación por; ordenación por; límite | sí | attested — https://learn.microsoft.com/es-es/fabric/database/sql/query | WHERE, GROUP BY, ORDER BY y LIMIT permanecen literales como palabras clave; la explicación se traduce. |
| projection; selection (select list); alias; expression; window expression | proyección; selección (lista SELECT); alias; expresión; expresión de ventana | no | attested — https://learn.microsoft.com/es-es/fabric/database/sql/query | Alias es una palabra incorporada al español; SELECT permanece como palabra clave. |
| aggregate; function; table function; scalar | agregado; función; función de tabla; escalar | no | attested — https://learn.microsoft.com/es-es/fabric/database/sql/query | Agregado/agregada concuerda con el sustantivo; función es femenino. |
| raw SQL; SQL template; SQL fragment | SQL sin formato; plantilla SQL; fragmento SQL | no | attested — https://learn.microsoft.com/es-es/contribute/content/text-formatting-guidelines | Se traduce el concepto y se conserva el acrónimo SQL; evitar «SQL crudo». |
| parameter; bound parameter; placeholder; query parameter | parámetro; parámetro vinculado; marcador de posición; parámetro de consulta | no | attested — https://learn.microsoft.com/es-es/power-platform/power-fx/expression-grammar | «Parámetro» es masculino; no usar placeholder en prosa. |
| compile; compiled SQL | compilar; SQL compilado | no | attested — https://learn.microsoft.com/es-es/contribute/content/text-formatting-guidelines | Compilar y compilado se traducen; SQL permanece como acrónimo. |
| thenable | thenable | sí | attested — https://developer.mozilla.org/es/docs/Web/JavaScript/Reference/Global_Objects/Promise/then | Término de protocolo de JavaScript que MDN deja en inglés. |
| helper; namespace; factory; callback; iterator; variadic | función auxiliar; espacio de nombres; fábrica; función de retorno de llamada; iterador; variádico | no | attested — https://developer.mozilla.org/es/docs/Learn_web_development/Extensions/Server-side/Express_Nodejs/Introduction | MDN usa también callback, pero la prosa debe usar la forma española; variádico/variádica concuerda con el sustantivo. |
| insert; write; batch; bulk insert; insert from select | inserción; escritura; lote; inserción masiva; inserción desde SELECT | no | attested — https://learn.microsoft.com/es-es/sql/relational-databases/native-client-odbc-results/processing-results-odbc?view=sql-server-ver15 | Lote traduce batch; SELECT se conserva como palabra clave. |
| stream; streaming; row; result set; replacement (ReplacingMergeTree); versioning; deletion flag | flujo; procesamiento en flujo; fila; conjunto de resultados; reemplazo (ReplacingMergeTree); versionado; indicador de borrado | no | inferred — https://learn.microsoft.com/es-es/sql/relational-databases/native-client-odbc-results/processing-results-odbc?view=sql-server-ver15 | Se distinguen flujo concreto y procesamiento en flujo; ReplacingMergeTree es nombre propio. |
| client; connection; server; replica; request; response; transport; HTTP request | cliente; conexión; servidor; réplica; solicitud; respuesta; transporte; solicitud HTTP | no | attested — https://developer.mozilla.org/es/docs/Learn_web_development/Extensions/Server-side/First_steps/Client-Server_overview | Solicitud/respuesta son las formas de MDN; HTTP se conserva como acrónimo. |
| timeout; settings; configuration; config; option | tiempo de espera; ajustes; configuración; configuración; opción | no | attested — https://learn.microsoft.com/es-es/biztalk/adapters-and-accelerators/adapter-sql/configure-transaction-isolation-level-and-transaction-timeout-with-sql | Config se desarrolla en prosa; los nombres config se conservan. «Ajustes» no se confunde con «configuración». |
| session; temporary-table workflow; transaction; concurrency; lifecycle; cleanup | sesión; flujo de trabajo con tablas temporales; transacción; concurrencia; ciclo de vida; limpieza | no | attested — https://www.fundeu.es/recomendacion/flujo-de-trabajo-mejor-que-workflow/ | FundéuRAE recomienda «flujo de trabajo» para workflow. |
| runtime method; execute; fetch; abort; cancel | método de ejecución; ejecutar; obtener; abortar; cancelar | no | inferred — https://learn.microsoft.com/es-es/style-guide/global-communications/writing-tips | «Método» es masculino; los demás son infinitivos. Confirmar «método de ejecución» con un nativo. |
| observability; tracing; logging; metrics; instrumentation; event | observabilidad; trazado; registro; métricas; instrumentación; evento | no | attested — https://learn.microsoft.com/es-es/azure/well-architected/operational-excellence/observability | Trazado, registro e instrumentación son las formas de la documentación técnica española. |
| span; logger; hook | span; logger; hook | sí | attested — https://opentelemetry.io/es/docs/concepts/instrumentation/libraries/ | OpenTelemetry en español deja estos términos de API en inglés; plural: spans, loggers, hooks. |
| plugin | plugin | sí | inferred — sin fuente abierta | No se abrió una fuente que atestigüe el término; se deja en inglés por coherencia con el campo de API `plugins` y con hook, que ya se conserva; plural: plugins. Confirmar con un nativo si se prefiere «complemento». |
| error; exception; throws; rejects; error model; validation error; stack; retry | error; excepción; lanza; rechaza; modelo de errores; error de validación; pila; reintento | no | attested — https://learn.microsoft.com/es-es/windows/win32/debug/error-message-guidelines | Error es una palabra incorporada al español; «pila» se usa en «pila de llamadas». |
| security; SQL injection; untrusted input; identifier quoting; escaping | seguridad; inyección SQL; entrada no fiable; entrecomillado de identificadores; escape | no | attested — https://learn.microsoft.com/es-es/troubleshoot/windows-client/networking/use-authenticated-proxy-servers | SQL se conserva; en prosa escaping se explica como «secuencias de escape». |
| credentials; password; allow-list; permission | credenciales; contraseña; lista de permitidos; permiso | no | attested — https://learn.microsoft.com/es-es/troubleshoot/windows-client/networking/use-authenticated-proxy-servers | «Permitidos» concuerda con el sustantivo implícito «elementos». |
| installation; install; version; pin a version; release | instalación; instalar; versión; fijar una versión; lanzamiento | no | attested — https://learn.microsoft.com/es-es/entra/msal/javascript/react/migration-guide-v4-v5 | Microsoft usa estas formas. |
| peer dependency | peer dependency | yes | decided by the project owner | Se conserva en inglés: es el nombre del campo `peerDependencies` de npm y así se trata también en zh-Hant-HK. Microsoft usa «dependencia del mismo nivel», pero no se adopta. En prosa va sin código ni comillas, en femenino por «dependencia»: «una peer dependency». |
| breaking change; before 1.0; pre-release; migration (schema migration); production | cambio incompatible; antes de 1.0; versión preliminar; migración (de esquema); producción | no | attested — https://learn.microsoft.com/es-es/style-guide/global-communications/writing-tips | Versión y migración son femeninos; 1.0 es un identificador. |
| example; runnable; reference; documentation; page; section | ejemplo; ejecutable; referencia; documentación; página; sección | no | attested — https://learn.microsoft.com/es-es/style-guide/global-communications/writing-tips | Ejecutable/ejecutables concuerda con el sustantivo; el resto son sustantivos españoles. |
| caveat; note; behavior; semantics; contract | salvedad; nota; comportamiento; semántica; contrato | no | unverified | Salvedad es femenino; puede glosarse como «advertencia» según el contexto; confirmar con un nativo. |
| mode; stable; internal; deprecated | modo; estable; interno; obsoleto | no | attested — https://learn.microsoft.com/es-es/style-guide/checklists/capitalization-checklist | Modo, estable, interno y obsoleto se traducen y concuerdan con el sustantivo. |
| public API | API pública | sí | attested — https://learn.microsoft.com/es-es/contribute/content/text-formatting-guidelines | API se conserva; «pública» concuerda con API. |
| shape; surface; layer | forma; superficie; capa | no | unverified | Formas descriptivas: forma de datos, superficie de API y capa de arquitectura; confirmar con un nativo. |
| keeps X easy; without hiding; make everyday access easier to structure | hace que X siga siendo sencillo; sin ocultar; hace más fácil organizar el acceso habitual | no | attested — https://learn.microsoft.com/es-es/style-guide/word-choice/use-simple-words-concise-sentences | Ajusta sencillo/sencilla al sustantivo de X; son equivalencias idiomáticas, no calcos. |
| is rejected; is not validated; is decoded conservatively | se rechaza; no se valida; se decodifica con cautela | no | attested — https://learn.microsoft.com/es-es/windows/win32/debug/error-message-guidelines | Pasiva refleja y «con cautela» conservan el sentido sin burocratizarlo. |
| carries; exposes; follows; stays; leaves; pass; wrap; compose; accepts; returns; throws | lleva; expone; sigue; se mantiene; deja; pasa; envuelve; compone; acepta; devuelve; lanza | no | attested — https://learn.microsoft.com/es-es/style-guide/word-choice/use-simple-words-concise-sentences | Usa el verbo concreto; «devuelve» es la forma elegida para returns y «lanza» para throws. |

**Anglicismos.** «Consulta» es la forma de la prosa SQL; query se reserva
para identificadores y nombres de API. «Framework» y «software» se mantienen:
Vue y MDN los usan en sus páginas españolas, y RAE documenta software y
hardware como voces extendidas. [Microsoft consultas](https://learn.microsoft.com/es-es/fabric/database/sql/query),
[Vue](https://es.vuejs.org/v2/guide/), [MDN](https://developer.mozilla.org/es/docs/Web/JavaScript),
[RAE software](https://temporalraeweb.rae.es/dhle/software),
[RAE hardware](https://temporalraeweb.rae.es/dhle/hardware). «Builder» no se
traduce como «constructor» si es el nombre de una clase o API; en prosa,
«constructor de consultas» explica el concepto. «Constructor» sin contexto se
reserva para una clase, forma usada por Microsoft y MDN. [Microsoft C#](https://learn.microsoft.com/es-es/dotnet/csharp/programming-guide/classes-and-structs/using-constructors),
[MDN constructor](https://developer.mozilla.org/es/docs/Web/JavaScript/Reference/Classes/constructor)

### 7. Wording guard

Estas formas generan un aviso de estilo en la prosa es-ES; no son errores
gramaticales automáticos. La guardia no se aplica a citas, datos de usuario ni
código. Las dos primeras filas son preferencias de variante: las fuentes
respaldan tanto la forma marcada como la preferida, por eso tienen nivel WARN.
Los nombres de producto solo se bloquean si alguien los traduce.

| Do not use | Preferred | Reason and source |
|---|---|---|
| computadora | ordenador | FundéuRAE documenta ambas formas y señala «ordenador» como la usada en España; es una preferencia de variante. [FundéuRAE](https://www.fundeu.es/consulta/computadoraordenador-1215/) |
| ustedes como plural informal | vosotros | FundéuRAE documenta la distribución de ambas formas y la presencia de «ustedes» en algunas zonas de España; es una preferencia de variante. [FundéuRAE](https://www.fundeu.es/consulta/tu-vos-usted-49/) |
| se procederá a | haz, ejecuta, se hará | Registro administrativo y agente oculto. [Microsoft](https://learn.microsoft.com/es-es/style-guide/global-communications/writing-tips) |
| el usuario deberá | tienes que, debes, imperativo | Aleja el texto del lector. [Microsoft person](https://learn.microsoft.com/es-es/style-guide/grammar/person) |
| Casa de clics, TipoScript o traducción de ClickHouse/TypeScript | ClickHouse, TypeScript | Los nombres propios se conservan literalmente. [ClickHouse](https://clickhouse.com/), [TypeScript](https://www.typescriptlang.org/) |

### 8. Model passage

#### Modelo mental

Ten en cuenta estos límites al usar `ck-orm`:

- `ckType.*` define los tipos de columna del esquema.
- `ckTable(...)` define los esquemas de las tablas.
- `ck.*` es el espacio de nombres de las funciones auxiliares de consulta.
- `fn.*` es el espacio de nombres de las funciones auxiliares de SQL.
- El esquema describe las tablas y las columnas, no el nombre de la base de datos.
- La conexión con la base de datos está en `clickhouseClient(...)`.
- Las consultas del constructor son la vía predeterminada; puedes usar SQL sin formato para expresiones específicas de ClickHouse.
- `runInSession()` es una función auxiliar de sesión de ClickHouse, no una transacción.
- `leftJoin()` usa la semántica SQL de NULL de forma predeterminada.
- Los resultados numéricos grandes o de alto riesgo se decodifican con cautela, en vez de convertirse silenciosamente.

#### Instalación

ck-orm no tiene dependencias obligatorias. `@opentelemetry/api` es una peer dependency opcional que solo usa el plugin `ck-orm/otel`; instálala junto con ck-orm cuando uses ese plugin.

### 9. Sources consulted

- [Microsoft style-guide index](https://learn.microsoft.com/ca-es/globalization/reference/microsoft-style-guides): variantes es-ES/es-MX.
- [FundéuRAE, computadora/ordenador](https://www.fundeu.es/consulta/computadoraordenador-1215/): ambas formas son correctas y «ordenador» se usa en España.
- [Microsoft person](https://learn.microsoft.com/es-es/style-guide/grammar/person), [writing tips](https://learn.microsoft.com/es-es/style-guide/global-communications/writing-tips): segunda persona, claridad y voz activa.
- [Microsoft text formatting](https://learn.microsoft.com/es-es/contribute/content/text-formatting-guidelines): código, nombres de paquetes e identificadores.
- [MDN, arrays](https://developer.mozilla.org/es/docs/Glossary/Array), [MDN, colecciones indexadas](https://developer.mozilla.org/es/docs/Web/JavaScript/Guide/Indexed_collections): «matriz», «arreglo» y el nombre de API Array.
- [MDN, cliente-servidor](https://developer.mozilla.org/es/docs/Learn_web_development/Extensions/Server-side/First_steps/Client-Server_overview), [MDN, Express/Node](https://developer.mozilla.org/es/docs/Learn_web_development/Extensions/Server-side/Express_Nodejs/Introduction): solicitudes, respuestas, sesiones y funciones de retorno de llamada.
- [Microsoft, dependencia del mismo nivel](https://learn.microsoft.com/es-es/entra/msal/javascript/react/migration-guide-v4-v5): forma atestiguada de peer dependency; no se adopta, se conserva el término en inglés por decisión del proyecto.
- [Microsoft, conjuntos de resultados](https://learn.microsoft.com/es-es/sql/relational-databases/native-client-odbc-results/processing-results-odbc?view=sql-server-ver15): filas y conjuntos de resultados.
- [Microsoft, observabilidad](https://learn.microsoft.com/es-es/azure/well-architected/operational-excellence/observability), [OpenTelemetry, librerías](https://opentelemetry.io/es/docs/concepts/instrumentation/libraries/), [OpenTelemetry, logs](https://opentelemetry.io/es/docs/concepts/signals/logs/): observabilidad, trazas, spans, hooks y loggers.
- [FundéuRAE, flujo de trabajo](https://www.fundeu.es/recomendacion/flujo-de-trabajo-mejor-que-workflow/): equivalencia de workflow.
- [Microsoft sentence capitalization](https://learn.microsoft.com/es-es/style-guide/text-formatting/using-type/use-sentence-style-capitalization): sentence case.
- [Microsoft punctuation](https://learn.microsoft.com/es-es/globalization/internationalization/punctuation-separators), [dashes](https://learn.microsoft.com/es-es/style-guide/punctuation/dashes-hyphens/): signos y rayas.
- [FundéuRAE comillas](https://www.fundeu.es/recomendacion/comillas-uso-de-este-signo-ortografico/), [RAE dos puntos](https://www.rae.es/ortografía/los-dos-puntos), [RAE paréntesis](https://www.rae.es/ortografía/los-paréntesis-como-signo-delimitador): puntuación.
- [FundéuRAE decimales](https://www.fundeu.es/recomendacion/decimales-coma-y-punto-son-ambas-validas/), [millares](https://www.fundeu.es/consulta/miles-6498/), [porcentaje](https://www.fundeu.es/consulta/grafia-del-simbolo-del-porcentaje-2742/): números y espacios.
- [Microsoft global formats](https://learn.microsoft.com/es-es/windows/apps/design/globalizing/use-global-ready-formats): fechas, horas y números locales.
- [RAE, gerundio de conclusión](https://www.rae.es/libro-estilo-justicia/las-palabras-y-sus-grupos-problemas-y-actuaciones/gerundio/usos-incorrectos/gerundio-de-conclusión): advertencia sobre gerundios que expresan posterioridad.
- [FundéuRAE, por ejemplo](https://www.fundeu.es/consulta/por-ejemplo-1687/): p. ej. y puntos/espacios de abreviaturas.
- [Microsoft SQL](https://learn.microsoft.com/es-es/fabric/database/sql/query), [Vue](https://es.vuejs.org/v2/guide/), [MDN](https://developer.mozilla.org/es/docs/Web/JavaScript), [React](https://es.react.dev/learn): terminología técnica y la elección de «matriz».
- [RAE software](https://temporalraeweb.rae.es/dhle/software) y [hardware](https://temporalraeweb.rae.es/dhle/hardware): anglicismos extendidos.
- [FundéuRAE, en base a](https://www.fundeu.es/recomendacion/con-base-en-base-a-sobre-la-base-de-basandose-en/): alternativas preferibles a «en base a».

### 10. Open questions

- Confirmar con un desarrollador español si «matriz» es preferible a «array» en la prosa de tipos; la tabla usa «matriz» y conserva Array solo como API.
- Confirmar «flujo» frente a «procesamiento en flujo» para stream y streaming en documentación específica de ClickHouse.
- Confirmar «salvedad» para caveat, «pila» para stack y «variádico» para variadic; son formas españolas concretas, pero no se ha encontrado una preferencia es-ES única para cada una.
- Confirmar si span, hook, logger y thenable deben conservarse sin glosa en la primera aparición; las fuentes técnicas españolas los dejan en inglés, pero el uso varía por equipo.
- La elección de ordenador frente a computadora y de vosotros frente a ustedes es de variante; la guardia queda en WARN para no marcar como incorrecta una forma legítima.
- No se ha encontrado una fuente única de es-ES que normalice todos los nombres de tipos de ClickHouse; se ha priorizado una forma española concreta y se marca como inferred o unverified cuando la evidencia no es directa.


