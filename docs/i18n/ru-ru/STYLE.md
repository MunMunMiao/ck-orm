## ru-RU - Русский (Россия)

### 1. Variant and reader

Это русский язык России (ru-RU) для разработчиков, которые работают с TypeScript,
JavaScript, ClickHouse и SQL. Текст должен звучать как объяснение коллеги:
точно, спокойно и без канцелярита. Не переносите украинские, белорусские или
машинно-переводные формы без отдельного подтверждения.

Ориентир по регистру — разговорная, но профессиональная техническая документация:
короткие предложения, активный залог, один термин для одного понятия и главное
утверждение в начале. Это согласуется с рекомендациями Microsoft: использовать
естественный язык, повседневную лексику, краткие фразы и активные конструкции.
См. <https://learn.microsoft.com/ru-ru/contribute/content/style-quick-start>
и <https://learn.microsoft.com/ru-ru/windows/apps/design/style/writing-style>.

### 2. Address, register and voice

- Обращайтесь к читателю на «вы», со строчной буквы внутри предложения.
  Тон вежливый, но прямой; не добавляйте «пожалуйста» в каждую инструкцию.
  Microsoft прямо рекомендует в русском интерфейсе форму «вы»:
  <https://learn.microsoft.com/ru-ru/windows/apps/design/style/writing-style>.
- В инструкциях используйте повелительное наклонение во множественном числе:
  «Используйте», «Передайте», «Вызовите», «Проверьте». Начинайте с действия,
  когда читателю нужно что-то сделать; такой прямой подход следует принципам
  Microsoft для инструкций и интерфейсного текста:
  <https://learn.microsoft.com/ru-ru/windows/apps/design/style/writing-style>.
- `you can` переводите по смыслу: «можно», если описывается доступный вариант,
  и «вы можете», если важно подчеркнуть действие читателя. `you should` обычно
  становится прямым «используйте», «проверьте» или «сделайте».
- Начинайте сразу с результата или действия. Не используйте «В этой статье мы
  рассмотрим…», «Следует отметить…» и формальные заключительные пожелания.
  Завершайте ссылкой на следующий шаг: «См. раздел …», «Теперь можно …».
- Предпочитайте короткие предложения и активный залог. Разбивайте длинное
  предложение, если в нём несколько действий или условий. Это следует
  рекомендациям Microsoft для ясного русского текста:
  <https://learn.microsoft.com/ru-ru/contribute/content/style-quick-start>.

Четыре калибровочных примера:

| English source sentence | BAD: официально-деловой или калькированный вариант | Хороший вариант |
|---|---|---|
| The design goal is straightforward: make everyday ClickHouse access easier to structure without hiding the parts that make ClickHouse different. | **BAD.** Целью дизайна является осуществление упрощения структурирования повседневного доступа к ClickHouse без сокрытия аспектов, делающих ClickHouse отличающимся. | Цель простая: удобнее организовать повседневную работу с ClickHouse и не скрывать его особенности. |
| Builder queries are thenable, so `await query` executes the query directly. | **BAD.** Запросы, сформированные посредством билдера, являются thenable, вследствие чего осуществляется непосредственное выполнение запроса посредством `await query`. | Запросы билдера поддерживают `await`, поэтому `await query` сразу выполняет запрос. |
| Use `registerTempTable(name)` when the temporary table is created by a command you control but you still want `ck-orm` to drop it during session cleanup: | **BAD.** Используйте `registerTempTable(name)` в случае, если временная таблица создаётся контролируемой вами командой, однако вы всё же желаете, чтобы `ck-orm` осуществлял её удаление в процессе очистки сессии: | Вызовите `registerTempTable(name)`, если временную таблицу создаёт ваша команда, но удалить её при очистке сессии должен `ck-orm`. |
| If you need true parallelism, use different `session_id` values instead of raising the same-session limit | **BAD.** В случае необходимости обеспечения истинного параллелизма следует использовать различные значения `session_id` вместо повышения лимита запросов в рамках одной сессии. | Если нужен настоящий параллелизм, используйте разные `session_id`, а не повышайте лимит для одной сессии. |

### 3. Words and patterns to avoid

В переводной прозе избегайте канцелярских связок, если простой глагол передаёт
тот же смысл. Список ниже — редакционное применение рекомендаций Microsoft о
повседневной лексике, краткости и активном изложении:
<https://learn.microsoft.com/ru-ru/contribute/content/style-quick-start>
и <https://learn.microsoft.com/ru-ru/windows/apps/design/style/writing-style>.

| Avoid | Why | Say instead |
|---|---|---|
| данный, данная, данное | Указательное слово раздувает фразу и может спутаться со значением «данные». | этот, эта, это; или ничего |
| осуществлять, осуществляется | Канцелярская оболочка скрывает действие. | делать, выполнять, запускать; активный глагол |
| производить проверку/настройку/установку | Отглагольное существительное с лишним «производить». | проверять, настраивать, устанавливать |
| в целях | Формальный эквивалент простого «для» или «чтобы». | для, чтобы |
| посредством | Тяжёлая замена предлога «через» или оборота «с помощью». | через, с помощью |
| является, являются | Часто лишняя связка; делает фразу статичной. | это, имеет тип, поддерживает; либо убрать |
| в рамках | Абстрактная рамка вместо точного контекста. | в, при, для; назвать конкретный объект |
| в части | Канцелярская замена уточнения. | о, для, в; назвать свойство |
| в настоящее время | Длинная замена, не добавляющая смысла. | сейчас; обычно убрать |
| имеет место | Формальная замена «есть» или «происходит». | есть, происходит, возникает |
| не представляется возможным | Маскирует запрет или ошибку. | нельзя, не удаётся, невозможно |
| в случае если | Двойной условный оборот. | если |
| следует отметить | Пустая вводная формула, отодвигающая факт. | сразу сообщить факт |
| необходимо осуществить | Два канцелярских слоя вместо действия. | нужно сделать; сделайте |
| задействовать | Универсальный бюрократический глагол без точного действия. | использовать, включить, вызвать |
| вышеуказанный, нижеследующий | Юридический регистр и лишняя навигация. | этот, следующий, указанный ниже; или ссылка |
| в дальнейшем | Неясно, когда именно. | позже, после этого, при следующем запуске |
| посредством которого | Тяжёлая относительная конструкция. | через который, с помощью которого |

Слова из этой таблицы могут быть грамматически уместны в цитате, названии API
или юридическом тексте. Правило относится к локализованной прозе, а не к коду.

### 4. Punctuation, typography and spacing

- Используйте русские внешние кавычки «ёлочки», а внутри них — „лапки“.
  Кавычки не отделяйте пробелами. Это правило подтверждают Gramota.ru и
  типографический стандарт:
  <https://gramota.ru/biblioteka/spravochniki/pismovnik/v-chem-raznitsa-mezhdu-kavychkami-raznogo-risunka>
  и <https://normadocs.ru/gost_r_7.0.110-2025>.
- Не ставьте пробел перед точкой, запятой, двоеточием, точкой с запятой,
  вопросительным или восклицательным знаком; после них ставьте один пробел.
  После открывающей и перед закрывающей скобкой пробела нет. Не допускайте
  двойных пробелов: <https://normadocs.ru/gost_r_7.0.110-2025>.
- В обычном тексте используйте тире `—` с пробелами. В диапазонах дат, чисел и
  версий используйте `–` без пробелов; дефис `-` оставляйте для сложных слов
  (`64-битный`) и идентификаторов. Это различение зафиксировано в
  <https://normadocs.ru/gost_r_7.0.110-2025>.
- Не вставляйте ручные мягкие переносы в русскую прозу: перенос оставляйте
  рендереру. Дефис внутри слова и знак переноса строки — разные вещи; это
  редакционное правило проекта на основе типографических требований
  <https://normadocs.ru/gost_r_7.0.110-2025>.
- Используйте один символ многоточия `…`, а не три точки, если многоточие
  действительно нужно. В коде, строковом литерале и URL ничего не
  типографизируйте. Правило для знака пропуска приведено в
  <https://normadocs.ru/gost_r_7.0.110-2025>.
- Перед маркированным или нумерованным списком и перед блоком кода ставьте
  двоеточие, если предшествующее предложение его вводит. Пункты-полные
  предложения заканчивайте точкой;
  короткие фрагменты оформляйте единообразно без точки. Для самостоятельных
  предложений и заголовков Microsoft также различает конечную точку:
  <https://learn.microsoft.com/ru-ru/style-guide/punctuation/periods>.
- Заголовки пишите в sentence case: с прописной буквы только первое слово и
  имена собственные, без конечной точки. Не набирайте заголовки вручную
  капсом. `docs.headline` в этом проекте всё равно визуально переводится в
  верхний регистр; русские слова длиннее английских, поэтому этот заголовок и
  другие UI-строки держите короткими. Основание для sentence case и запрета
  сплошного капса:
  <https://learn.microsoft.com/ru-ru/style-guide/text-formatting/using-type/use-sentence-style-capitalization>.
- Вставляйте неразрывный пробел там, где перенос ломает смысл: между числом и
  единицей, числом и `%`, `№` и номером, а также при необходимости между
  коротким предлогом и следующим словом. Для единиц, процентов и групп цифр
  это прямо следует из <https://normadocs.ru/gost_r_7.0.110-2025>.
- Код, имена пакетов, идентификаторы, URL и строковые литералы не переводите и
  не изменяйте ради русской морфологии: пишите «в `ck-orm`», «из `ck-orm`»,
  «с `ck-orm`», а не добавляйте окончание внутрь кодового фрагмента. Вне кодового фрагмента
  допустимы устоявшиеся сочетания `SQL-запрос`, `HTTP-запрос`, `API-метод`.
  Это проектное правило выведено из требования сохранять имена продуктов и
  техническое форматирование; фиксированные написания сверяйте с
  <https://clickhouse.com/> и <https://www.typescriptlang.org/>.
- Жирный и курсив используйте только для акцента; идентификаторы оформляйте
  кодовым фрагментом, а не кавычками. Не смешивайте латиницу и кириллицу в одном
  идентификаторе и не подменяйте латинские буквы похожими кириллическими.
  Кавычки, скобки и кодовые строки должны сохранять исходный вид по
  <https://normadocs.ru/gost_r_7.0.110-2025>.
- В русской прозе используйте Unicode-знаки «полной» типографики: `«»`,
  `„“`, `—`, `–`, `…`, а не случайные ASCII-подстановки вроде `""`, `--` и `...`.
  В кодовом фрагменте, URL и машинных значениях сохраняйте ASCII-форму исходника.
  Это редакционное применение правил кавычек, тире и многоточия из
  <https://normadocs.ru/gost_r_7.0.110-2025>.
- Пишите `ё` во всех русских словах, где она нормативно предусмотрена; в коде,
  названиях пакетов и цитатах сохраняйте исходное написание. Это более
  последовательное решение проекта; при этом в обычной печати `ё` часто
  употребляют выборочно, а обязательной она становится там, где без неё можно
  неправильно прочитать слово. Исключение: «экспортер» (компонент OpenTelemetry) пишется без `ё`, как в русской
  документации ClickHouse и в постах разработчиков. См. <https://gramota.ru/biblioteka/spravochniki/pravila-russkoy-orfografii-i-punktuatsii/upotreblenie-bukvy-e-v-tekstakh-raznogo-naznacheniya>.

### 5. Numbers, units and dates

- В русской прозе десятичный разделитель — запятая: `3,14`. В коде, SQL,
  версиях и других машинных значениях оставляйте исходную точку: `3.14`,
  `0.1.0`. Русские числовые форматы и разделители описаны в
  <https://learn.microsoft.com/ru-ru/dotnet/api/system.globalization.numberformatinfo?view=net-10.0>
  и <https://normadocs.ru/gost_r_7.0.110-2025>.
- Для длинных чисел используйте неразрывные группы цифр: `1 234 567`.
  Идентификаторы, версии, `session_id` и коды не разбивайте и не группируйте.
  Основание: <https://normadocs.ru/gost_r_7.0.110-2025>.
- Между числом и единицей ставьте пробел, лучше неразрывный: `64 бит`,
  `10 мс`, `5 %`, `20 °C`. Знак градуса пишется слитно с числом только в
  угловой мере: `20°`. Правила единиц:
  <https://protect.gost.ru/gost/details/fa1165d5-0547-429a-9c3e-f16f4bdb101f>.
- Сложные определения оформляйте через дефис: `64-битное целое`,
  `32-разрядный процесс`. Порядковые формы с цифрой используйте только при
  необходимости (`1-й`); в обычной прозе предпочтительнее «первый». Для
  дефиса и диапазона действует <https://normadocs.ru/gost_r_7.0.110-2025>;
  выбор «первый» вместо частых цифровых сокращений — редакционное правило.
- Процент в тексте пишите `5 %`, с пробелом; в кодовом фрагменте и исходном SQL
  сохраняйте оригинальный формат. Gramota.ru отдельно подтверждает пробел
  перед `%`: <https://gramota.ru/poisk?mode=spravka&query=%D0%B7%D0%BD%D0%B0%D0%BA%D0%BE%D0%BC%20%D0%BF%D1%80%D0%BE%D1%86%D0%B5%D0%BD%D1%82>.
- Пишите «например» полностью. Обычно: «например, …» и «…, например, …»;
  не ставьте запятые, если оборот тесно встроен в значение. Это правило
  приведено в справочниках русской орфографии; в этом руководстве оно также
  нужно, чтобы не раздувать текст сокращениями.
- В датах используйте `12 октября 2026 г.`; названия месяцев со строчной
  буквы. Для журналов, API и машинных полей сохраняйте ISO-формат
  `2026-10-12T14:30:00Z`, `UTC` и исходные цифры. Диапазон пишите
  `10–12 октября`, без пробелов вокруг тире; машинные даты не локализуйте.
  Формат диапазонов и пробелы подтверждает
  <https://normadocs.ru/gost_r_7.0.110-2025>; сохранение ISO — правило
  проекта, поскольку это значение данных.

### 6. Terminology

В этой таблице `keep English? = yes` означает не «источник не найден», а
подтверждённый термин отрасли, имя продукта, идентификатор, API или ключевое
слово, которое разработчики оставляют в английском виде. Для остальных понятий
всегда указана русская форма. `attested` означает, что форма есть в указанном
источнике; `inferred` — что она выведена из нескольких близких русских
технических источников; `unverified` — что это обычная словарная форма без
достаточной профильной фиксации и её должен проверить носитель.

| English | local rendering | keep English? | status | source / note |
|---|---|---|---|---|
| schema | схема | no | attested | <https://learn.microsoft.com/ru-ru/sql/relational-databases/tables/tables?view=sql-server-ver17> |
| boundary | граница | no | inferred | Для conceptual boundary выбрана русская форма «граница»; в инструкции сохраняйте множественное число «границы». |
| database | база данных | no | attested | <https://learn.microsoft.com/ru-ru/sql/relational-databases/security/permissions-database-engine?view=sql-server-ver17> |
| database name | имя базы данных | no | inferred | Из русских SQL-описаний базы данных и имени объекта. |
| database connection | подключение к базе данных | no | attested | <https://learn.microsoft.com/ru-ru/dotnet/api/system.net.http.httpclient.getstringasync?view=net-8.0> |
| column | столбец | no | attested | <https://learn.microsoft.com/ru-ru/sql/relational-databases/security/permissions-database-engine?view=sql-server-ver17> |
| table | таблица | no | attested | <https://learn.microsoft.com/ru-ru/sql/relational-databases/security/permissions-database-engine?view=sql-server-ver17> |
| temporary table | временная таблица | no | attested | <https://learn.microsoft.com/ru-ru/sql/t-sql/statements/create-table-transact-sql?view=sql-server-ver17> |
| table engine | движок таблицы | no | inferred | Из русскоязычного употребления «движок» для database engine и «таблица» в Microsoft SQL docs. |
| column type | тип столбца | no | attested | <https://learn.microsoft.com/ru-ru/sql/relational-databases/indexes/index-properties-f1-help?view=sql-server-ver17> |
| type definition | определение типа | no | attested | <https://learn.microsoft.com/ru-ru/dotnet/csharp/language-reference/language-specification/types> |
| nullable | допускающий `NULL` | no | inferred | Из русских описаний nullable-типов и фиксированного SQL-имени `NULL`; не заменяйте на «нулевой». |
| optional | необязательный | no | attested | <https://learn.microsoft.com/ru-ru/sql/t-sql/statements/alter-credential-transact-sql?view=sql-server-ver17> |
| required | обязательный | no | attested | <https://learn.microsoft.com/ru-ru/entra/identity/authentication/tutorial-configure-custom-password-protection> |
| default value | значение по умолчанию | no | attested | <https://learn.microsoft.com/ru-ru/sql/t-sql/statements/create-table-transact-sql?view=sql-server-ver17> |
| modifier | модификатор | no | inferred | Обычная русская форма в документации языков программирования; конкретная форма для ClickHouse не закреплена. |
| materialized column | материализованный столбец | no | unverified | Без профильного русскоязычного источника; это буквальная техническая форма, проверить у носителя. |
| alias column | столбец с псевдонимом | no | unverified | Без профильного русскоязычного источника; предпочтена описательная форма вместо «столбец-алиас». |
| array | массив | no | attested | <https://developer.mozilla.org/ru/docs/Web/JavaScript/Guide/Data_structures> |
| tuple | кортеж | no | attested | <https://learn.microsoft.com/ru-ru/dotnet/csharp/language-reference/builtin-types/value-tuples> |
| map | тип `Map` | yes | attested | `Map` — имя JavaScript/ClickHouse-типа; русская форма «карта» в этом значении не используется. <https://developer.mozilla.org/ru/docs/Web/JavaScript/Reference/Global_Objects/Map> |
| nested | вложенный тип | no | inferred | Из русских технических описаний вложенных структур; `Nested` как имя типа сохраняйте в кодовом фрагменте. |
| enum | перечисление | no | attested | <https://metanit.com/web/typescript/2.5.php> |
| literal | литерал | no | attested | <https://metanit.com/web/typescript/2.5.php> |
| string | строка | no | attested | <https://developer.mozilla.org/ru/docs/Web/JavaScript/Guide/Data_structures> |
| integer | целое число | no | attested | <https://developer.mozilla.org/ru/docs/Web/JavaScript/Guide/Data_structures> |
| 64-bit integer | 64-битное целое число | no | inferred | Русская форма сложного определения по правилам раздела 5; `64-bit` в прозе оформляйте через дефис. |
| floating point | число с плавающей точкой | no | attested | <https://developer.mozilla.org/ru/docs/Web/JavaScript/Guide/Data_structures> |
| boolean | логическое значение | no | attested | <https://developer.mozilla.org/ru/docs/Web/JavaScript/Guide/Data_structures> |
| date | дата | no | attested | <https://developer.mozilla.org/ru/docs/Web/JavaScript/Guide/Data_structures> |
| timestamp | временная метка | no | attested | <https://developer.mozilla.org/ru/docs/Web/JavaScript/Guide/Data_structures> |
| timezone | часовой пояс | no | attested | <https://developer.mozilla.org/ru/docs/Web/JavaScript/Guide/Data_structures> |
| precision | точность | no | attested | <https://learn.microsoft.com/ru-ru/sql/t-sql/data-types/precision-scale-and-length-transact-sql?view=sql-server-ver17> |
| scale | масштаб | no | attested | <https://learn.microsoft.com/ru-ru/sql/t-sql/data-types/precision-scale-and-length-transact-sql?view=sql-server-ver17> |
| exact value | точное значение | no | inferred | Из русских описаний точных числовых типов в Microsoft Learn и контекста `Decimal`. |
| lossless | без потери точности | no | inferred | Из русских описаний точных числовых типов и противопоставления потере точности в Microsoft Learn. |
| JSON path | путь JSON | no | inferred | Из русской формы «путь» для адресации данных; `JSON` — фиксированное имя формата. |
| generic type | обобщённый тип | no | attested | <https://metanit.com/web/typescript/3.5.php> |
| type inference | вывод типов | no | attested | <https://learn.microsoft.com/ru-ru/dotnet/csharp/language-reference/builtin-types/value-tuples> |
| inferred | выведенный | no | attested | <https://learn.microsoft.com/ru-ru/dotnet/csharp/language-reference/builtin-types/value-tuples> |
| typed | типизированный | no | attested | <https://metanit.com/web/typescript/3.5.php> |
| untyped | нетипизированный | no | inferred | Морфологическое образование от зафиксированного «типизированный»; профильную русскую форму проверить. |
| type assertion | утверждение типа | no | attested | <https://metanit.com/web/typescript/3.5.php> |
| runtime validation | проверка во время выполнения | no | inferred | Из русских MDN- и Microsoft-описаний runtime-проверок; не сокращайте до «валидация». |
| validator | валидатор | no | inferred | Русская техническая форма по употреблению «валидировать» и `validator` в документации; проверить для продуктового глоссария. |
| encoder | кодировщик | no | attested | <https://developer.mozilla.org/ru/docs/Web/API/TextEncoder> |
| decoder | декодировщик | no | attested | <https://developer.mozilla.org/ru/docs/Web/API/TextDecoder> |
| decoding | декодирование | no | attested | <https://developer.mozilla.org/ru/docs/Web/API/TextDecoder> |
| query | запрос | no | attested | <https://developer.mozilla.org/ru/docs/Learn_web_development/Core/Scripting/Network_requests> |
| query builder | построитель запросов | no | inferred | По русской форме «построитель» в документации библиотек и фиксированной форме «запрос». |
| builder | билдер | no | attested | Живое употребление российскими разработчиками: <https://habr.com/ru/companies/rshb/articles/779150/>; мужской род: «билдера», «билдером». |
| chain | цепочка вызовов | no | inferred | Из русских описаний цепочек вызовов в документации JavaScript; не используйте «чейн». |
| chainable | поддерживающий цепочку вызовов | no | inferred | Описательный вариант по смыслу `chain`; профильную краткую форму проверить. |
| subquery | подзапрос | no | attested | <https://learn.microsoft.com/ru-ru/sql/relational-databases/performance/subqueries?view=sql-server-ver17> |
| bare subquery | самостоятельный подзапрос | no | unverified | Без источника с точным этим различием; выбрана буквальная описательная форма, проверить у носителя. |
| anonymous CTE | безымянный CTE | no | inferred | Из русской формы «безымянный» и фиксированного SQL-акронима `CTE`. |
| join | соединение | no | attested | <https://learn.microsoft.com/ru-ru/sql/relational-databases/performance/joins?view=sql-server-ver17> |
| left join | левое соединение | no | attested | <https://learn.microsoft.com/ru-ru/sql/relational-databases/performance/joins?view=sql-server-ver17> |
| outer join | внешнее соединение | no | attested | <https://learn.microsoft.com/ru-ru/sql/relational-databases/performance/joins?view=sql-server-ver17> |
| null semantics | семантика `NULL` | no | inferred | Из русских SQL-описаний семантики `NULL`; `NULL` сохраняется как SQL-ключевое слово. |
| predicate | предикат | no | attested | <https://learn.microsoft.com/ru-ru/sql/relational-databases/security/row-level-security?view=sql-server-ver17> |
| filter | фильтр | no | attested | <https://developer.mozilla.org/ru/docs/Web/JavaScript/Reference/Global_Objects/Array/filter> |
| where clause | условие `WHERE` | no | inferred | Из русских SQL-руководств и фиксированного ключевого слова `WHERE`. |
| group by | группировка `GROUP BY` | no | inferred | Из русских SQL-руководств; `GROUP BY` сохраняется как ключевое слово. |
| order by | сортировка `ORDER BY` | no | inferred | Из русских SQL-руководств; `ORDER BY` сохраняется как ключевое слово. |
| limit | ограничение `LIMIT` | no | inferred | Из русских описаний ограничения числа строк; `LIMIT` сохраняется как SQL-имя. |
| projection | проекция | no | unverified | Без единого русскоязычного API-источника для этого значения; проверить на примерах `ck-orm`. |
| selection (select list) | список выбираемых выражений (`SELECT`) | no | unverified | Без источника с точным различием между selection и результатом; `SELECT` — SQL-ключевое слово. |
| alias | псевдоним | no | attested | <https://learn.microsoft.com/ru-ru/sql/relational-databases/performance/joins?view=sql-server-ver17> |
| expression | выражение | no | attested | <https://learn.microsoft.com/ru-ru/sql/t-sql/language-elements/expressions-transact-sql?view=sql-server-ver17> |
| window expression | оконное выражение | no | inferred | Из русской SQL-терминологии для оконных функций и формы «выражение». |
| aggregate | агрегат | no | inferred | Из русской SQL-терминологии; при необходимости уточняйте «агрегатная функция». |
| function | функция | no | attested | <https://ru.vuejs.org/guide/introduction.html> |
| table function | табличная функция | no | inferred | Из русских SQL-описаний табличных функций; профильную форму ClickHouse проверить. |
| scalar | скалярное значение | no | attested | <https://learn.microsoft.com/ru-ru/dax/dax-queries> |
| raw SQL | сырой SQL | no | inferred | Устоявшаяся русская форма в документации ORM; `SQL` — фиксированный акроним. |
| ClickHouse-specific expression | выражение, специфичное для ClickHouse | no | inferred | Идиоматическая русская форма для технической особенности продукта; имя ClickHouse не переводится. |
| SQL template | SQL-шаблон | no | inferred | Композиция русской формы «шаблон» с фиксированным `SQL`; дефис обязателен. |
| SQL fragment | фрагмент SQL | no | inferred | Композиция русской формы «фрагмент» с фиксированным `SQL`. |
| parameter | параметр | no | attested | <https://developer.mozilla.org/ru/docs/Web/API/Fetch_API/Using_Fetch> |
| bound parameter | параметр с привязанным значением | no | unverified | Без источника с точной ORM-формой; выбрана понятная описательная форма. |
| placeholder | плейсхолдер | no | inferred | Употребление российскими разработчиками; мужской род: «плейсхолдера», «плейсхолдером»; источник для точной русской формы не закрепляет нормативный вариант. |
| query parameter | параметр запроса | no | attested | <https://developer.mozilla.org/ru/docs/Web/API/Fetch_API/Using_Fetch> |
| compile | компилировать | no | attested | <https://metanit.com/web/typescript/3.5.php> |
| compiled SQL | скомпилированный SQL | no | inferred | Из русской формы «скомпилированный» и фиксированного `SQL`. |
| thenable | `thenable` | yes | attested | Термин JavaScript оставляют в английском виде в русскоязычных материалах разработчиков: <https://habr.com/ru/companies/jugru/articles/441140/>, <https://developer.mozilla.org/en-US/docs/Glossary/Thenable>; при первом употреблении поясняйте «объект с методом `then`». |
| helper | вспомогательная функция | no | inferred | Из русских React/Vue-описаний вспомогательных функций: <https://ru.react.dev/learn>; не оставляйте `helper` в прозе. |
| namespace | пространство имён | no | attested | <https://ru.react.dev/learn> |
| factory | фабрика | no | attested | <https://developer.mozilla.org/ru/docs/Web/JavaScript/Guide/Iterators_and_generators> |
| callback | функция обратного вызова | no | attested | <https://developer.mozilla.org/ru/docs/Web/API/EventTarget/addEventListener> |
| iterator | итератор | no | attested | <https://developer.mozilla.org/ru/docs/Web/JavaScript/Reference/Iteration_protocols> |
| variadic | с переменным числом аргументов | no | inferred | Описательная русская форма из терминологии функций с переменным числом параметров. |
| insert | вставка | no | attested | <https://learn.microsoft.com/ru-ru/sql/t-sql/statements/insert-transact-sql?view=sql-server-ver17> |
| write | запись | no | attested | <https://learn.microsoft.com/ru-ru/sql/relational-databases/tables/modify-data?view=sql-server-ver17> |
| batch | пакетная операция | no | inferred | Из русской документации о пакетной обработке; для записи уточняйте «пакетная вставка». |
| bulk insert | массовая вставка | no | attested | <https://learn.microsoft.com/ru-ru/sql/relational-databases/import-export/bulk-import-large-data-sets-by-using-bulk-insert-or-openrowset-bulk-sql-server?view=sql-server-ver17> |
| insert from select | вставка из `SELECT` | no | inferred | Из русской формы «вставка» и фиксированного SQL-ключевого слова `SELECT`. |
| stream | поток | no | attested | <https://developer.mozilla.org/ru/docs/Web/API/Streams_API> |
| streaming | потоковая передача | no | attested | <https://developer.mozilla.org/ru/docs/Web/API/Streams_API> |
| row | строка | no | attested | <https://learn.microsoft.com/ru-ru/sql/relational-databases/security/row-level-security?view=sql-server-ver17> |
| result set | набор результатов | no | attested | <https://learn.microsoft.com/ru-ru/sql/relational-databases/native-client-odbc-results/processing-results?view=sql-server-ver17> |
| replacement (ReplacingMergeTree) | замена (`ReplacingMergeTree`) | no | unverified | Без русскоязычного ClickHouse-глоссария; имя движка не переводится, «замена» — буквальная форма. |
| versioning | версионирование | no | inferred | Из русской технической формы «версионирование»; профильную форму для `ReplacingMergeTree` проверить. |
| deletion flag | флаг удаления | no | inferred | Из русских описаний признаков удаления и формы «флаг». |
| client | клиент | no | attested | <https://developer.mozilla.org/ru/docs/Web/API/Fetch_API/Using_Fetch> |
| connection | соединение | no | attested | <https://developer.mozilla.org/ru/docs/Web/API/XMLHttpRequest> |
| server | сервер | no | attested | <https://developer.mozilla.org/ru/docs/Learn_web_development/Core/Scripting/Network_requests> |
| replica | реплика | no | attested | <https://learn.microsoft.com/ru-ru/sql/database-engine/availability-groups/windows/overview-of-always-on-availability-groups-sql-server?view=sql-server-ver17> |
| request | запрос | no | attested | <https://developer.mozilla.org/ru/docs/Web/API/Fetch_API/Using_Fetch> |
| response | ответ | no | attested | <https://developer.mozilla.org/ru/docs/Web/API/XMLHttpRequest> |
| transport | транспорт | no | inferred | Из русской сетевой терминологии; не заменяйте на «перевозка». |
| HTTP request | HTTP-запрос | no | attested | <https://developer.mozilla.org/ru/docs/Learn_web_development/Core/Scripting/Network_requests> |
| timeout | таймаут | no | attested | MDN использует «таймаут», Microsoft — «время ожидания»: <https://developer.mozilla.org/ru/docs/Web/API/Window/setTimeout>, <https://learn.microsoft.com/ru-ru/dotnet/api/system.net.http.httpclient.getstringasync?view=net-8.0> |
| settings | настройки | no | attested | <https://learn.microsoft.com/ru-ru/azure/azure-resource-manager/management/azure-subscription-service-limits> |
| configuration | конфигурация | no | attested | <https://learn.microsoft.com/ru-ru/entra/identity/authentication/tutorial-enable-sspr-writeback> |
| config | конфигурация | no | inferred | В прозе раскрывайте `config` как «конфигурация»; исходное слово оставляйте только в имени параметра. |
| option | опция | no | attested | <https://ru.vuejs.org/guide/introduction.html> |
| session | сессия | no | inferred | Употребление российскими разработчиками и в ClickHouse-материалах; женский род: «сессии», «сессию»; «сеанс» допустим в общем интерфейсном тексте. |
| temporary-table workflow | сценарий работы с временной таблицей | no | inferred | Описательная форма по русским словам «сценарий», «работа» и «временная таблица». |
| transaction | транзакция | no | attested | <https://learn.microsoft.com/ru-ru/sql/t-sql/language-elements/transactions-transact-sql?view=sql-server-ver17> |
| concurrency | параллелизм | no | inferred | Из русской терминологии параллельного выполнения; не путайте с «транзакцией». |
| lifecycle | жизненный цикл | no | attested | <https://ru.vuejs.org/guide/introduction.html> |
| cleanup | очистка | no | inferred | Русская форма из описаний очистки ресурсов; конкретную форму ClickHouse проверить. |
| runtime method | метод выполнения | no | inferred | Описательная форма для метода, вызываемого во время выполнения; не «рантайм-метод». |
| execute | выполнить | no | attested | <https://developer.mozilla.org/ru/docs/Web/JavaScript/Guide/Iterators_and_generators> |
| fetch | получить | no | attested | <https://developer.mozilla.org/ru/docs/Web/API/Fetch_API/Using_Fetch> |
| abort | прервать | no | attested | <https://developer.mozilla.org/ru/docs/Web/API/XMLHttpRequest> |
| cancel | отменить | no | attested | <https://developer.mozilla.org/ru/docs/Web/API/Window/setTimeout> |
| retry | повторная попытка | no | inferred | Русская форма в технических руководствах; для глагола используйте «повторить». |
| observability | наблюдаемость | no | attested | Microsoft ru-ru, «Наблюдаемость .NET с помощью OpenTelemetry»: <https://learn.microsoft.com/ru-ru/dotnet/core/diagnostics/observability-with-otel>. Русская документация ClickHouse пишет «обсервабилити»; эта форма не принята. |
| tracing | трассировка | no | attested | <https://learn.microsoft.com/ru-ru/azure/azure-monitor/app/distributed-tracing>; та же форма в русской документации ClickHouse: <https://clickhouse.com/docs/ru/operations/opentelemetry> |
| span | спан | no | attested | Основные источники: <https://clickhouse.com/docs/ru/operations/opentelemetry>, <https://clickhouse.com/docs/ru/operations/system-tables/opentelemetry_span_log>, <https://yandex.cloud/ru/docs/monium/traces/concepts>; употребление российскими разработчиками: <https://habr.com/ru/post/574322/>; мужской род: «спана», «спаном». Страницы Microsoft с машинным переводом смешивают «интервал» и «диапазон»; эти формы не приняты. |
| logging | логирование | no | attested | <https://learn.microsoft.com/ru-ru/azure/azure-monitor/app/api-custom-events-metrics> |
| logger | логгер | no | inferred | Из русского developer-употребления «логгер» и документации по logging; проверить предпочтительный стиль. |
| metrics | метрики | no | attested | <https://learn.microsoft.com/ru-ru/azure/azure-monitor/app/api-custom-events-metrics> |
| instrumentation | инструментирование | no | inferred | Из русской терминологии OpenTelemetry и профилирования: Microsoft и русская документация ClickHouse пишут «инструментирование»; Yandex Cloud пишет «Инструментация» — этот вариант не принят. Не оставляйте `instrumentation` в прозе. |
| hook | хук | no | attested | Официальные русские React/Vue-документы используют «хук»: <https://ru.react.dev/learn>, <https://ru.vuejs.org/guide/introduction.html>; мужской род: «хука», «хуком». Microsoft ru-ru также пишет «стартового хука». |
| plugin | плагин | no | attested | Русская документация Vue: <https://ru.vuejs.org/guide/reusability/plugins>. |
| exporter | экспортер | no | attested | Русская документация ClickHouse, страница об интеграции OpenTelemetry (12 употреблений): <https://clickhouse.com/docs/ru/observability/integrating-opentelemetry>; Microsoft пишет «Экспортеры». Источники пишут без `ё`, и это принято как исключение из правила раздела 4: в поиске по X (grok-4.7, 2026-10-15) в недавних постах разработчиков встречается «экспортер» или английское exporter, «экспортёр» — только заголовок Habr 2019 года. |
| Collector | коллектор | no | attested | Русская документация ClickHouse: <https://clickhouse.com/docs/ru/observability/integrating-opentelemetry>. Название продукта «OpenTelemetry Collector» остаётся английским. |
| SDK | SDK | yes | attested | Акроним остаётся английским и не склоняется: «запустите SDK», «в SDK». |
| tracer provider | провайдер трассировщика | no | unverified | Источника нет. Русский машинный перевод Microsoft даёт «поставщика трассировки». Имя API-класса `TracerProvider` остаётся в кодовом фрагменте. |
| context manager | менеджер контекста | no | unverified | Только документация Python, где «менеджер контекста» относится к оператору `with`. |
| propagator | propagator | yes | inferred | Остаётся латиницей, без обратных кавычек. Ни один русский источник форму «пропагатор» не использует; в поиске по X (grok-4.7, 2026-10-15; посты отдельно не перепроверялись) «пропагатор» встречается только в значении «пропагандист», а русскоязычные разработчики оставляют OpenTelemetry, OTEL и Prometheus на английском. Машинный перевод Microsoft даёт «распространители», ClickHouse пишет «Передача контекста трассировки». |
| SpanKind | `SpanKind` | yes | attested | Имя API; русская документация ClickHouse оставляет его английским: «SpanKind спана», <https://clickhouse.com/docs/ru/operations/system-tables/opentelemetry_span_log>. Слово «тип» не используйте: оно путается с типом данных. |
| sampling | семплирование | no | attested | Yandex Cloud пишет «Семплирование», русская документация ClickHouse — «Сэмплирование», Microsoft — «Выборка». Только для семплирования трассировок и спанов: «выборка» в значениях SQL и статистики остаётся. |
| entry point | точка входа | no | attested | Страница Microsoft о методе `Main` в C#. |
| bundle | бандл | no | attested | Как существительное: русская документация Vue, страница о производительности («бандлов»): <https://ru.vuejs.org/guide/best-practices/performance>. Сочетание «браузерный бандл» не подтверждено источником. |
| worker | воркер | no | attested | MDN: <https://developer.mozilla.org/ru/docs/Web/API/Web_Workers_API>. Сочетание «edge-воркер» с префиксом не подтверждено источником. |
| environment variable | переменная окружения | no | attested | Документация Yandex Cloud. |
| preflight | предварительный запрос (preflight) | no | attested | MDN, раздел о CORS («Предварительные запросы»): <https://developer.mozilla.org/ru/docs/Web/HTTP/Guides/CORS>. При первом употреблении добавляйте «(preflight)». |
| statement | оператор | no | inferred | Источники расходятся: Microsoft пишет «Инструкция SQL», документация Python — «инструкции», русская документация ClickHouse — «Операторы SQL». Выбран «оператор», чтобы не пересекаться с «командой» (command) и «запросом» (query). |
| set up (heading) | Настройка | no | unverified | Слабый источник: только заголовок на странице Microsoft с машинным переводом. |
| event | событие | no | attested | <https://developer.mozilla.org/ru/docs/Web/API/EventTarget/addEventListener> |
| error | ошибка | no | attested | <https://developer.mozilla.org/ru/docs/Web/JavaScript/Reference/Errors/Invalid_for-in_initializer> |
| exception | исключение | no | attested | <https://developer.mozilla.org/ru/docs/Web/JavaScript/Reference/Errors/Invalid_for-in_initializer> |
| throws | выбрасывает | no | attested | <https://developer.mozilla.org/ru/docs/Web/JavaScript/Reference/Errors/Invalid_for-in_initializer> |
| rejects | отклоняет | no | attested | <https://developer.mozilla.org/ru/docs/Web/JavaScript/Reference/Global_Objects/Promise> |
| error model | модель ошибок | no | inferred | Из русской формы «модель ошибок» в документации библиотек и общего слова «ошибка». |
| validation error | ошибка проверки | no | inferred | Выбрано вместо кальки «ошибка валидации» для обычной прозы; термин «валидатор» отдельно допустим. |
| stack | стек | no | attested | <https://developer.mozilla.org/ru/docs/Web/JavaScript/Reference/Global_Objects/Error/stack> |
| security | безопасность | no | attested | <https://learn.microsoft.com/ru-ru/azure/synapse-analytics/sql-data-warehouse/column-level-security> |
| SQL injection | SQL-инъекция | no | attested | <https://learn.microsoft.com/ru-ru/sql/relational-databases/security/sql-injection?view=sql-server-ver17> |
| untrusted input | непроверенный ввод | no | inferred | Описательная форма из русской security-терминологии; избегает английского «input». |
| identifier quoting | заключение идентификаторов в кавычки | no | unverified | Без источника с точной русской формой; проверить у носителя и владельца API. |
| escaping | экранирование | no | attested | <https://developer.mozilla.org/ru/docs/Web/JavaScript/Guide/Grammar_and_types> |
| credentials | учётные данные | no | attested | <https://learn.microsoft.com/ru-ru/entra/identity/authentication/tutorial-configure-custom-password-protection> |
| password | пароль | no | attested | <https://learn.microsoft.com/ru-ru/entra/identity/authentication/tutorial-configure-custom-password-protection> |
| allow-list | список разрешённых значений | no | inferred | Из русской формы «список разрешённых» в документации безопасности; не используйте `allow-list` в прозе. |
| permission | разрешение | no | attested | <https://learn.microsoft.com/ru-ru/sql/relational-databases/security/permissions-database-engine?view=sql-server-ver17> |
| installation | установка | no | attested | <https://learn.microsoft.com/ru-ru/contribute/content/style-quick-start> |
| install | установить | no | attested | <https://learn.microsoft.com/ru-ru/contribute/content/style-quick-start> |
| peer dependency | peer-зависимость | yes | attested | Это npm-термин отрасли: русская документация pnpm использует «peer-зависимости»: <https://pnpm.io/ru/how-peers-are-resolved>; женский род: «одна peer-зависимость», «peer-зависимости». |
| version | версия | no | attested | <https://learn.microsoft.com/ru-ru/contribute/content/style-quick-start> |
| pin a version | зафиксировать версию | no | inferred | Из русского developer-употребления «зафиксировать версию» и смысла pinning. |
| release | выпуск | no | attested | <https://learn.microsoft.com/ru-ru/contribute/content/style-quick-start> |
| breaking change | несовместимое изменение | no | inferred | Из русской документации о совместимости версий; не используйте «брейкинг». |
| before 1.0 | до 1.0 | no | inferred | Прямая русская форма; номер версии сохраняется без изменений. |
| pre-release | предварительный выпуск | no | attested | <https://learn.microsoft.com/ru-ru/azure/devops/release-notes/2020/sprint-170-update> |
| migration (schema migration) | миграция схемы | no | attested | <https://learn.microsoft.com/ru-ru/ef/core/managing-schemas/migrations/> |
| production | продакшен | no | inferred | Живое российское developer-употребление; Microsoft чаще пишет «рабочая среда». Для единообразия этой документации выбран «продакшен». |
| example | пример | no | attested | <https://ru.react.dev/learn> |
| runnable | запускаемый | no | inferred | Из русской формы «запустить» и описаний запускаемых примеров; проверить у носителя. |
| reference | справочник | no | attested | <https://learn.microsoft.com/ru-ru/dotnet/csharp/language-reference/builtin-types/value-tuples> |
| documentation | документация | no | attested | <https://ru.vuejs.org/guide/introduction.html> |
| page | страница | no | attested | <https://ru.react.dev/learn> |
| section | раздел | no | attested | <https://learn.microsoft.com/ru-ru/windows/apps/design/style/writing-style> |
| caveat | оговорка | no | inferred | Описательная русская форма для ограничения или важного условия; профильной терминологической фиксации не найдено. |
| note | примечание | no | attested | <https://learn.microsoft.com/ru-ru/windows/apps/design/style/writing-style> |
| behavior | поведение | no | attested | <https://developer.mozilla.org/ru/docs/Web/JavaScript/Reference/Iteration_protocols> |
| semantics | семантика | no | attested | <https://ru.vuejs.org/guide/introduction.html> |
| contract | контракт | no | inferred | Русская форма в API-документации; для пользовательской прозы не заменяйте на «договор». |
| mode | режим | no | attested | <https://developer.mozilla.org/ru/docs/Web/API/Fetch_API/Using_Fetch> |
| public API | публичный API | no | inferred | Из русской формы «публичный» и фиксированного API; `API` сохраняется как акроним. |
| stable | стабильный | no | attested | <https://learn.microsoft.com/ru-ru/contribute/content/style-quick-start> |
| internal | внутренний | no | attested | <https://learn.microsoft.com/ru-ru/windows/apps/design/style/writing-style> |
| deprecated | устаревший | no | attested | <https://learn.microsoft.com/ru-ru/windows/apps/design/style/writing-style> |
| shape | форма | no | inferred | Русская форма TypeScript-термина shape; при необходимости уточняйте «форма типа». |
| surface | внешняя часть API | no | unverified | Без единой профильной русской формы; выбрана описательная форма, проверить у носителя. |
| layer | слой | no | attested | <https://learn.microsoft.com/ru-ru/azure/synapse-analytics/sql-data-warehouse/column-level-security> |
| default path | путь по умолчанию | no | inferred | Из русской формы «по умолчанию» и технического слова «путь». |
| large numeric result | крупный числовой результат | no | inferred | Описательная форма для числового результата большого размера или диапазона; во множественном числе «крупные числовые результаты». |
| high-risk numeric result | потенциально опасный числовой результат | no | inferred | Описательная форма для результата, который может потерять точность или диапазон; во множественном числе «потенциально опасные числовые результаты». |
| automatically | автоматически | no | inferred | Русская форма наречия; не оставляйте `automatically` в прозе. |
| explicitly | явно | no | inferred | Русская форма наречия; не оставляйте `explicitly` в прозе. |
| keeps X easy | упрощает X | no | inferred | Идиоматическая глагольная форма вместо «сохраняет простоту X». |
| without hiding | не скрывая | no | inferred | Идиоматическая русская деепричастная конструкция по смыслу фразы. |
| make everyday access easier to structure | удобнее организовать повседневную работу с X | no | inferred | Идиоматическая перестройка английской именной цепочки; не переводите дословно «доступ». |
| is rejected | отклоняется | no | inferred | Обычная форма для значения «система не принимает»; в контексте ошибки допустимо «вызывает ошибку». |
| is not validated | не проверяется | no | inferred | Прямая русская форма для отсутствия проверки. |
| is decoded conservatively | декодируется осторожно | no | inferred | Идиоматическая русская форма с явным смыслом conservative decoding. |
| silently coerced | неявно приводится к другому типу | no | inferred | Явно передаёт отсутствие заметного для пользователя преобразования типа. |
| carries | передаёт | no | inferred | Выбран глагол по объекту «значение/данные», а не калька «несёт». |
| exposes | предоставляет | no | inferred | Для API естественнее «предоставляет», чем буквальное «выставляет наружу». |
| follows | следует | no | attested | <https://ru.vuejs.org/guide/introduction.html> |
| stays | остаётся | no | inferred | Идиоматическая форма для неизменности свойства или значения. |
| leaves | оставляет | no | inferred | Идиоматическая форма для сохранения поля или значения. |
| pass | передать | no | attested | <https://ru.react.dev/learn> |
| wrap | обернуть | no | inferred | Русская форма для оборачивания выражения или компонента. |
| compose | составить | no | inferred | Для предикатов и выражений используйте «составить» или «объединить» по контексту. |
| accepts | принимает | no | attested | <https://developer.mozilla.org/ru/docs/Web/API/EventTarget/addEventListener> |
| returns | возвращает | no | attested | <https://developer.mozilla.org/ru/docs/Web/API/XMLHttpRequest> |
| fixed product names and API names | без изменений | yes | attested | ClickHouse, ck-orm, TypeScript, JavaScript, SQL, JSON, Decimal, DDL, `NULL`, `CTE`, `HTTP`, `UTC`, OpenTelemetry, OpenTelemetry Collector, OTLP, W3C, Bun, npm, pnpm, Yarn, Node.js, Drizzle, GitHub, `llms.txt`, `MIT`, API names, identifiers и SQL keywords фиксированы правилами проекта: <https://clickhouse.com/>, <https://www.typescriptlang.org/>. Склоняйте их только снаружи кодового фрагмента. |

### 7. Wording guard

В локализованной русской прозе эти формы дают предупреждение о
канцелярском регистре. Они могут остаться в исходной цитате, имени метода или
нормативной выдержке. Основа — требования к естественному языку, краткости и
активным конструкциям в руководствах Microsoft:
<https://learn.microsoft.com/ru-ru/contribute/content/style-quick-start>
и <https://learn.microsoft.com/ru-ru/windows/apps/design/style/writing-style>.

| Do not use in translated prose | Preferred |
|---|---|
| данный / данная / данное | этот / эта / это |
| осуществлять / осуществляется | делать / выполнять / активный глагол |
| производить проверку / настройку / установку | проверять / настраивать / устанавливать |
| в целях | для / чтобы |
| посредством | через / с помощью |
| является | это / поддерживает / имеет |
| не представляется возможным | нельзя / не удаётся |
| в рамках | в / при / для |
| в части | о / для / в |
| в настоящее время | сейчас / убрать |
| имеет место | есть / происходит |
| в случае если | если |
| следует отметить | сразу сообщить факт |
| необходимо осуществить | нужно сделать / сделайте |
| задействовать | использовать / включить / вызвать |
| вышеуказанный / нижеследующий | этот / следующий / ссылка |

Региональные замены не добавлены без отдельного проверенного источника:
не приписывайте русскому языку формы другой национальной локали по памяти.
В JSON-поле `contamination` правил сейчас нет записей: добавляйте их только
при наличии источника для обеих сопоставляемых форм и помечайте `WARN`, если
разницу не подтверждают два независимых источника.

### 8. Model passage

Учитывайте эти границы при работе с `ck-orm`:

- `ckType.*` задаёт типы столбцов схемы.
- `ckTable(...)` задаёт схемы таблиц.
- `ck.*` — пространство имён вспомогательных функций для запросов.
- `fn.*` — пространство имён вспомогательных функций SQL.
- Схема описывает таблицы и столбцы, но не имя базы данных.
- Подключение к базе данных задаётся в `clickhouseClient(...)`.
- По умолчанию используйте запросы, собранные билдером; для выражений, специфичных для ClickHouse, доступен сырой SQL.
- `runInSession()` — вспомогательная функция для сессий ClickHouse, а не транзакция.
- `plugins` наблюдают за каждым запросом, который отправляет ck-orm; трассировка включается по желанию через плагин `ck-orm/otel`, который не входит в основную точку входа
- `leftJoin()` по умолчанию использует семантику NULL в стиле SQL.
- Крупные или потенциально опасные числовые результаты декодируются осторожно, а не неявно приводятся к другому типу.

У ck-orm нет обязательных зависимостей. Трассировка OpenTelemetry вынесена в отдельную точку входа `ck-orm/otel`, а `@opentelemetry/api` — необязательная peer-зависимость, которую использует только она. Основная точка входа `ck-orm` никогда не импортирует OpenTelemetry, поэтому, если трассировка не нужна, приложение, браузерный бандл или edge-воркер не устанавливают и не включают в сборку ничего из OpenTelemetry. Чтобы трассировать запросы, выполните настройку из раздела [Плагин OpenTelemetry](#opentelemetry-plugin).

### 9. Sources consulted

- <https://learn.microsoft.com/ru-ru/contribute/content/style-quick-start> —
  естественный тон, краткость, прямое обращение и активные формулировки.
- <https://learn.microsoft.com/ru-ru/windows/apps/design/style/writing-style> —
  «вы», короткие инструкции, активный залог, оформление UI-текста.
- <https://learn.microsoft.com/ru-ru/style-guide/punctuation/periods> —
  точки в предложениях, списках и заголовках.
- <https://gramota.ru/biblioteka/spravochniki/pismovnik/v-chem-raznitsa-mezhdu-kavychkami-raznogo-risunka>
  — русские «ёлочки» и вложенные „лапки“.
- <https://gramota.ru/biblioteka/spravochniki/pravila-russkoy-orfografii-i-punktuatsii/upotreblenie-bukvy-e-v-tekstakh-raznogo-naznacheniya>
  — случаи, когда `ё` нужна для правильного чтения.
- <https://normadocs.ru/gost_r_7.0.110-2025> — пробелы, тире, дефисы,
  диапазоны, неразрывные пробелы, кавычки и заголовки.
- <https://protect.gost.ru/gost/details/fa1165d5-0547-429a-9c3e-f16f4bdb101f>
  — обозначения единиц и пробел между числом и единицей.
- <https://developer.mozilla.org/ru/docs/Glossary>,
  <https://developer.mozilla.org/ru/docs/Glossary/Promise> и
  <https://developer.mozilla.org/ru/docs/Web/JavaScript/Guide/Data_structures>
  — русские формы JavaScript-терминов, `null`, Promise и структур данных.
- <https://developer.mozilla.org/ru/docs/Web/API/Fetch_API/Using_Fetch> и
  <https://developer.mozilla.org/ru/docs/Web/API/XMLHttpRequest> — формы
  «запрос», «ответ», «параметр», «учётные данные» и сетевые термины.
- <https://developer.mozilla.org/ru/docs/Web/JavaScript/Reference/Iteration_protocols>
  — формы «итератор» и «перебор».
- <https://metanit.com/web/typescript/2.5.php> и
  <https://metanit.com/web/typescript/3.5.php> — формы терминов типов,
  кортежей, перечислений и обобщений в русскоязычной документации TypeScript.
- <https://ru.react.dev/learn> и <https://ru.vuejs.org/guide/introduction.html>
  — формы «обработчик», «хук», «состояние», «компонент», «фреймворк».
- <https://learn.microsoft.com/ru-ru/sql/t-sql/data-types/precision-scale-and-length-transact-sql?view=sql-server-ver17>
  — формы «точность» и «масштаб».
- <https://learn.microsoft.com/ru-ru/sql/relational-databases/security/permissions-database-engine?view=sql-server-ver17>
  — формы «столбец», «разрешение» и SQL-термины безопасности.
- <https://pnpm.io/ru/how-peers-are-resolved> — форма «peer-зависимость».
- <https://clickhouse.com/docs/ru/operations/opentelemetry>,
  <https://clickhouse.com/docs/ru/operations/system-tables/opentelemetry_span_log>
  и <https://clickhouse.com/docs/ru/observability/integrating-opentelemetry> —
  «трассировка», «спан», «коллектор», «экспортер», `SpanKind`, «сэмплирование».
- <https://yandex.cloud/ru/docs/monium/traces/concepts> — «спан»; документация
  Yandex Cloud также даёт «Семплирование», «переменная окружения» и
  «Инструментация».
- <https://learn.microsoft.com/ru-ru/dotnet/core/diagnostics/observability-with-otel>
  — «наблюдаемость» (страницы Microsoft с машинным переводом считаются слабой
  опорой).
- <https://ru.vuejs.org/guide/reusability/plugins> и
  <https://ru.vuejs.org/guide/best-practices/performance> — «плагин», «бандл».
- <https://developer.mozilla.org/ru/docs/Web/HTTP/Guides/CORS> и
  <https://developer.mozilla.org/ru/docs/Web/API/Web_Workers_API> —
  «предварительный запрос», «воркер».
- <https://habr.com/ru/companies/rshb/articles/779150/> и
  <https://habr.com/ru/post/574322/> и
  <https://habr.com/ru/companies/jugru/articles/441140/> — живое употребление
  «билдер», «запрос», «спан», `thenable` и других заимствований разработчиками
  в России.
- <https://clickhouse.com/> и <https://www.typescriptlang.org/> — официальные
  написания имён продуктов и языков, которые не транслитерируются.

### 10. Open questions

- Носитель должен подтвердить «материализованный столбец» и «столбец с
  псевдонимом» для ClickHouse: в локальном глоссарии могут предпочитаться
  другие формы.
- Проверьте у носителя различие между `projection`, «списком выбираемых
  выражений», «самостоятельным подзапросом» и «параметром с привязанным
  значением» на реальных примерах API.
- Уточните у носителя, естественнее ли для этой аудитории «заключение
  идентификаторов в кавычки» и «внешняя часть API», чем более короткие
  профессиональные формы.
- Подтвердите баланс «сессия» / «сеанс», а также падежи заимствований
  «билдер», «спан», «хук» и «плейсхолдер».
- Носитель должен выбрать между «провайдер трассировщика» и оставленным в коде
  `TracerProvider`: источника для русской формы нет.
- Носитель должен выбрать между «оператор» и «SQL-запрос» (и «инструкция»)
  для statement: источники расходятся, а «команда» и «запрос» уже заняты.
- Проверьте единообразное написание `ё` и ширину русской строки `docs.headline`.


