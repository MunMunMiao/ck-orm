## fr-FR - Français (France)

### 1. Variant and reader

Cette section vise les développeurs qui travaillent en France. Elle décrit le
français de France (fr-FR), pas le français canadien et pas un « français
international » sans couleur locale. Le registre reste professionnel, mais
celui d'une documentation lue par un développeur devant ouvrir un fichier,
lancer une commande et comprendre un type. Les guides français de Microsoft
recommandent un ton direct, simple et proche de la conversation.
[Microsoft, style rapide](https://learn.microsoft.com/fr-fr/contribute/content/style-quick-start)

Adressez-vous au lecteur avec `vous`, sans vouvoiement administratif : « Vous
pouvez... », « Passez... », « Vérifiez... ». En France, `requête`,
`sous-requête`, `jointure`, `session`, `client`, `serveur` et
`paramètre` sont les formes naturelles de la prose technique. Les pages
françaises de MDN, de React, de Vue et de TypeScript les attestent dans des
textes destinés aux développeurs. [MDN HTTP](https://developer.mozilla.org/fr/docs/Web/HTTP/Guides/Messages),
[React](https://fr.react.dev/learn),
[Vue](https://fr.vuejs.org/guide/introduction.html),
[TypeScript](https://www.typescriptlang.org/fr/docs/handbook/2/everyday-types.html)

`query builder` reste fréquent dans les équipes et les noms d'API françaises ;
`constructeur de requêtes` est la forme explicative attestée dans des
documentations françaises. Utilisez le premier quand il désigne le concept ou
une API communautaire, et le second quand la phrase doit être entièrement
française. [Yii, Query Builder](https://www.yiiframework.com/doc/guide/2.0/fr/db-query-builder),
[Microsoft, concepteur de requêtes](https://learn.microsoft.com/fr-fr/power-bi/paginated-reports/report-builder/relational-query-designer-user-interface-report-builder)

Les noms de produits, bibliothèques, API, identifiants, mots-clés et formats
restent littéraux, conformément aux décisions partagées : `ClickHouse`,
`ck-orm`, `TypeScript`, `JavaScript`, `SQL`, `JSON`, `Decimal`, `DDL`, `NULL`,
`CTE`, `HTTP`, `UTC`, `OpenTelemetry`, `Bun`, `npm`, `pnpm`, `Yarn`, `Node.js`,
`Drizzle`, `GitHub`, `llms.txt`, `MIT` et les noms d'API ne se francisent pas.
[Microsoft, formatage du texte](https://learn.microsoft.com/fr-fr/contribute/content/text-formatting-guidelines)

### 2. Address, register and voice

Employez `vous`, un ton simple et direct, des verbes actifs et des phrases
courtes. « Vous pouvez... » exprime une capacité ou une option ; ne le
transformez pas en « il sera possible de... ». Donnez une instruction avec
l'impératif (« Passez », « Exécutez », « Vérifiez ») et une explication avec
`vous`. Ouvrez une section par le résultat utile et terminez par la
conséquence pratique ou l'étape suivante. Les pages de référence n'ont besoin
ni de salutations ni de formules de clôture. [Microsoft, ton et clarté](https://learn.microsoft.com/fr-fr/contribute/content/style-quick-start)

| English source from `reference.md` | BAD (officialese/translationese) | Good (colloquial fr-FR) |
|---|---|---|
| `Use \`ck-orm\` with these boundaries in mind:` | **BAD:** Utilisez `ck-orm` en gardant à l'esprit les limites indiquées ci-dessous. | Gardez ces limites en tête quand vous utilisez `ck-orm` : |
| `The examples below use a single table so the main flow stays easy to follow.` | **BAD:** Les exemples ci-dessous utilisent une table unique afin que le flux principal demeure facile à suivre. | Les exemples ci-dessous utilisent une seule table pour que le déroulé principal reste facile à suivre. |
| `ClickHouse does not provide traditional transactions.` | **BAD:** Des transactions traditionnelles ne sont pas fournies par ClickHouse. | ClickHouse ne propose pas de transactions traditionnelles. |
| `Avoid returning \`responseText\` or the raw error \`message\` to untrusted clients.` | **BAD:** Il convient d'éviter de retourner `responseText` ou le `message` d'erreur brut aux clients non fiables. | Ne renvoyez pas `responseText` ni le `message` d'erreur brut à des clients qui ne sont pas de confiance. |

### 3. Words and patterns to avoid

Ces formes ne sont pas toutes grammaticalement fautives ; elles alourdissent
la documentation ou cachent l'agent. La règle est de choisir le verbe et la
préposition les plus courts quand le sens ne change pas. Microsoft recommande
les mots courants, les phrases concises et la voix active ; l'Académie française
déconseille aussi le calque `faire sens`. [Microsoft, mots simples](https://learn.microsoft.com/fr-fr/contribute/content/style-quick-start),
[Académie française, « faire »](https://www.academie-francaise.fr/faire)

| Avoid | Why | Say instead |
|---|---|---|
| `afin de` | Tour plus administratif que nécessaire. | `pour` |
| `au niveau de` | Locution vague quand il s'agit d'un objet ou d'un emplacement. | `dans`, `sur` ou le nom précis |
| `effectuer une opération` | Nominalisation bureaucratique. | `faire`, `lancer` ou `exécuter` |
| `procéder à` | Annonce l'action au lieu de la dire. | Le verbe direct : `créez`, `lisez`, `supprimez` |
| `il convient de` | Impersonnel et institutionnel. | L'impératif ou `vous devez` |
| `il est nécessaire de` | Long et abstrait quand une consigne suffit. | `vous devez` ou `il faut` |
| `dans le cadre de` | Calque administratif souvent remplaçable. | `pour`, `pendant` ou `dans` |
| `au sein de` | Solennel sans précision supplémentaire. | `dans` |
| `mettre en œuvre` | Périphrase ; elle masque souvent l'action technique. | `utiliser`, `activer` ou le verbe exact |
| `permettre de` | Acceptable, mais souvent moins direct que la capacité du lecteur. | `vous pouvez` ou le verbe précis |
| `a pour objectif de` | Nominalisation et répétition de l'intention. | `vise à` ou `sert à` |
| `faire sens` | Calque rejeté par l'Académie française. | `avoir du sens` |
| `dans le cas où` | Forme lourde dans une condition simple. | `si` |
| `de manière à` | Périphrase lorsqu'un but suffit. | `pour` ou `pour que` |
| `à l'issue de` | Registre administratif. | `après` |
| `en ce qui concerne` | Formule de transition vide. | `pour`, `sur` ou le terme précis |

Ces interdictions ciblent la prose explicative, pas les citations, les noms
d'API ni le texte de code. `Permettre de`, `au niveau de` et `mettre en œuvre`
restent possibles si leur sens propre est réellement nécessaire.

### 4. Punctuation, typography and spacing

- En prose, utilisez les guillemets français avec une espace fine insécable :
  `« requête »`. Utilisez le même caractère dans les deux guillemets, pas une
  espace ordinaire. Le choix de cette section est U+202F NARROW NO-BREAK SPACE,
  attesté pour la typographie française ; en Markdown UTF-8, écrivez le
  caractère littéralement. Si un générateur ne le conserve pas, utilisez
  `&#8239;` dans le HTML autorisé ou U+00A0 comme solution de repli. Testez
  l'affichage, car les espaces insécables peuvent gêner certains générateurs.
  [Unicode, U+202F](https://www.unicode.org/L2/L2019/19116-clarify-nnbsp.pdf)
- Placez U+202F avant `: ; !` et `?` : `Attention !`, `Type :`. Utilisez
  aussi U+202F pour les groupes de milliers et avant les unités. C'est une
  convention typographique française ; elle ne s'applique jamais à l'intérieur
  d'un identifiant, d'une URL, d'un bloc de code ou d'un code inline. [Unicode,
  texte français](https://www.unicode.org/L2/L2020/20008-core-text.pdf),
  [Académie française, nombres](https://www.academie-francaise.fr/questions-de-langue)
- La ponctuation de la prose reste de largeur normale : n'utilisez pas les
  signes pleine largeur. Entre un mot français, une lettre latine ou un nombre,
  mettez l'espace normal exigé par la phrase ; autour d'un code inline,
  laissez l'espace normal exigé par la phrase à l'extérieur et n'en ajoutez jamais dans les
  backticks. [Microsoft, formatage du texte](https://learn.microsoft.com/fr-fr/contribute/content/text-formatting-guidelines)
- Le Markdown de ce dépôt utilise l'apostrophe droite ASCII U+0027 (`'`) dans
  la prose source. Ne la remplacez pas par une apostrophe courbe et ne changez
  jamais l'apostrophe d'un littéral de code ; le but est de garder une source
  stable pour le générateur Markdown. [Microsoft, Markdown et apostrophes](https://learn.microsoft.com/fr-fr/contribute/content/markdown-reference),
  [Microsoft, remplacement des guillemets](https://learn.microsoft.com/fr-fr/contribute/content/docs-authoring/smart-quote-replacement)
- Utilisez `…` pour une vraie ellipse, sans espaces entre les points ; dans une
  documentation technique, terminez de préférence la phrase. Utilisez `—` pour
  une incise clairement délimitée, `–` pour une plage et `-` dans les identifiants
  ou noms de fichiers. Ne modifiez pas un signe dans le code. C'est une
  convention éditoriale du projet.
- Une phrase qui introduit une liste ou un bloc de code se termine par `:` ;
  mettez ensuite la liste ou le bloc sur la ligne suivante. N'ajoutez pas de
  deux-points à un titre ni à un libellé d'interface qui n'est pas une phrase.
  [Microsoft, contenu d'interface](https://learn.microsoft.com/fr-fr/power-platform/well-architected/experience-optimization/user-interface-content)
- Dans une liste de fragments, commencez en minuscule et omettez le point final.
  Dans une liste de phrases complètes, commencez par une majuscule et mettez un
  point à chaque élément. Gardez la même construction grammaticale d'un item à
  l'autre. [Microsoft, Markdown](https://learn.microsoft.com/fr-fr/contribute/content/markdown-reference)
- Les titres sont en sentence case : seule la première lettre et les noms
  propres prennent une majuscule. Pas de Title Case. `docs.headline` est rendu
  en capitales par l'interface : les capitales accentuées françaises sont
  correctes (`REQUÊTE`, `SÉCURITÉ`), mais le titre doit rester court pour ne pas
  déborder. [Microsoft, casse des titres](https://learn.microsoft.com/fr-fr/powershell/scripting/community/contributing/general-markdown?view=powershell-7.6)
- Utilisez les backticks pour le code, l'italique ou le gras seulement pour une
  emphase utile, et les parenthèses sans espace intérieur pour une précision
  brève. Si une parenthèse contient une explication longue, faites-en une
  phrase séparée. [Microsoft, style rapide](https://learn.microsoft.com/fr-fr/contribute/content/style-quick-start),
  [Microsoft, formatage du texte](https://learn.microsoft.com/fr-fr/contribute/content/text-formatting-guidelines)

### 5. Numbers, units and dates

- Dans la prose, utilisez la virgule décimale : `3,14`. Séparez les milliers
  par U+202F : `30 000` et `1 234 567`, jamais par un point ou une virgule.
  Dans un littéral SQL/JSON, une version ou une expression de code, conservez
  le point et la forme source : `0.1.0`, `1.25`, `Decimal(10, 2)`. [Académie
  française, nombres](https://www.academie-francaise.fr/questions-de-langue),
  [Microsoft, formats localisés](https://learn.microsoft.com/fr-fr/windows/apps/design/globalizing/use-global-ready-formats)
- Séparez le nombre de l'unité par une espace insécable : `500 ms`,
  `64 bits`, `25 %`, `128 KiB`. Les symboles d'unité n'ont ni point ni pluriel ;
  les noms d'unités écrits en toutes lettres peuvent prendre le pluriel. Écrivez
  `entier de 64 bits` en prose ; gardez `64-bit` dans un identifiant ou un code.
  C'est la convention de cette section ; la règle d'espace U+202F est détaillée
  plus haut.
- Les ordinaux abrégés ne sont nécessaires que pour une référence : `1er` au
  masculin, `1re` au féminin et `2e` pour les suivants. Si la phrase reste
  claire, écrivez plutôt « premier » ou « deuxième ». Cette convention n'a pas
  été vérifiée dans une source française en ligne suffisamment stable : à
  confirmer par une relecture native.
- Une version (`0.1.0`, `1.0.0`) est un identifiant, pas un nombre décimal.
  Gardez les points, les zéros et le préfixe `v` tels quels. Pour « for
  example », écrivez `par exemple` dans la prose, pas `e.g.` ; le code et les
  citations font exception. Pour les heures, utilisez `14:30` dans une
  documentation orientée développeurs ; les dates naturelles prennent la forme
  `12 octobre 2026`, tandis que les données échangées restent ISO :
  `2026-10-12T14:30:00Z`. [Microsoft, formats globalisés](https://learn.microsoft.com/fr-fr/windows/apps/design/globalizing/use-global-ready-formats)
### 6. Terminology

Chaque ligne fixe la forme à employer dans la prose. `keep English?` vaut `yes`
uniquement pour un nom de produit, un identifiant, un mot-clé SQL/API ou un
terme de l'écosystème JavaScript que les développeurs français laissent
effectivement en anglais. `unverified` ne signifie jamais « garder l'anglais » :
la forme française reste fournie.

| English | local rendering | keep English? | status | source / note |
|---|---|---:|---|---|
| ClickHouse; ck-orm; TypeScript; JavaScript; SQL; JSON; Decimal; DDL; NULL; CTE; HTTP; UTC; OpenTelemetry; Bun; npm; pnpm; Yarn; Node.js; Drizzle; GitHub; llms.txt; MIT | ClickHouse; ck-orm; TypeScript; JavaScript; SQL; JSON; Decimal; DDL; NULL; CTE; HTTP; UTC; OpenTelemetry; Bun; npm; pnpm; Yarn; Node.js; Drizzle; GitHub; llms.txt; MIT | yes | attested — [ClickHouse](https://clickhouse.com/), [TypeScript](https://www.typescriptlang.org/) | Noms de produits, formats et éléments fixés par la politique commune. |
| API names; identifiers; keywords | noms d'API; identifiants; mots-clés | yes | attested — [Microsoft, formatage du texte](https://learn.microsoft.com/fr-fr/contribute/content/text-formatting-guidelines) | Le nom littéral d'une API, d'un identifiant ou d'un mot-clé reste inchangé. |
| schema; schema DSL | schéma; DSL de schéma | no | attested — [TypeScript](https://www.typescriptlang.org/fr/docs/handbook/2/everyday-types.html) | `DSL` reste l'acronyme technique ; accord : `un schéma`. |
| column; table; temporary table; table engine | colonne; table; table temporaire; moteur de table | no | attested — [ClickHouse en français](https://clickhouse.com/fr/clickhouse) | Accords : `une colonne`, `une table`, `un moteur`. Les noms d'engine dans le code restent littéraux. |
| column type; type definition | type de colonne; définition de type | no | attested — [TypeScript](https://www.typescriptlang.org/fr/docs/handbook/2/basic-types.html) | `type` est masculin ; distinguez le type de sa définition. |
| nullable; optional; required; default value | qui accepte `NULL`; facultatif; obligatoire; valeur par défaut | no | attested — [TypeScript](https://www.typescriptlang.org/fr/docs/handbook/2/everyday-types.html), [ClickHouse](https://clickhouse.com/fr/clickhouse) | Accordez `facultatif` et `obligatoire` au nom décrit ; explicitez `nullable` plutôt que de le laisser en anglais. |
| modifier; materialized column; alias column | modificateur; colonne matérialisée; colonne alias | no | unverified | Forme française de base ; `matérialisée` s'accorde avec `colonne` et `alias` reste le nom technique français. |
| array; tuple; map; nested; enum | tableau; tuple; table associative; imbriqué; énumération | no | attested — [TypeScript, types objet](https://www.typescriptlang.org/fr/docs/handbook/2/objects.html), [ClickHouse](https://clickhouse.com/fr/clickhouse) | `tuple` est un terme français d'informatique ; `table associative` traduit le concept de `map`. `Imbriqué` s'accorde avec le type décrit. |
| literal; string; integer; 64-bit integer; floating point; boolean | littéral; chaîne de caractères; entier; entier de 64 bits; virgule flottante; booléen | no | attested — [TypeScript](https://www.typescriptlang.org/fr/docs/handbook/2/everyday-types.html) | Gardez `string`, `Int64` et les autres noms quand ce sont des types écrits. |
| date; timestamp; timezone | date; horodatage; fuseau horaire | no | attested — [Microsoft, formats globalisés](https://learn.microsoft.com/fr-fr/windows/apps/design/globalizing/use-global-ready-formats) | `un horodatage` et `un fuseau horaire`. |
| precision; scale; exact value; lossless | précision; échelle; valeur exacte; sans perte | no | unverified | Formes françaises de base ; vérifiez le sens précis d'`échelle` dans chaque API. |
| JSON path | chemin JSON | no | unverified | `JSON` reste le format fixe ; `chemin` est le nom français courant. |
| generic type; type inference / inferred; typed; untyped; type assertion | type générique; inférence de types / inféré; typé; non typé; assertion de type | no | attested — [TypeScript](https://www.typescriptlang.org/fr/docs/handbook/2/everyday-types.html) | Accordez `inféré`, `typé` et `non typé` au nom décrit. |
| runtime validation; validator | validation à l'exécution; validateur | no | attested — [TypeScript](https://www.typescriptlang.org/fr/docs/handbook/2/basic-types.html) | `à l'exécution` décrit le moment, contrairement à la vérification statique. |
| encoder; decoder; decoding | encodeur; décodeur; décodage | no | inferred — du vocabulaire français de l'encodage et du décodage employé par les documentations techniques | Noms masculins pour `encodeur` et `décodeur` ; ne gardez pas `decoder` dans la prose. |
| query | requête | no | attested — [MDN, messages HTTP](https://developer.mozilla.org/fr/docs/Web/HTTP/Guides/Messages), [Microsoft, concepteur de requêtes](https://learn.microsoft.com/fr-fr/power-bi/paginated-reports/report-builder/relational-query-designer-user-interface-report-builder) | `query` reste seulement dans un identifiant ou un nom d'API. |
| query builder | constructeur de requêtes | no | inferred — [Yii, Query Builder](https://www.yiiframework.com/doc/guide/2.0/fr/db-query-builder) et [Microsoft, concepteur de requêtes](https://learn.microsoft.com/fr-fr/power-bi/paginated-reports/report-builder/relational-query-designer-user-interface-report-builder) | `concepteur de requêtes` existe aussi ; retenez `constructeur de requêtes` dans ce guide. |
| builder | constructeur | no | inferred — de `constructeur de requêtes` et du vocabulaire français des API | Dans un nom d'API, conservez le nom littéral même si le concept est traduit ici. |
| chain / chainable | chaînage / chaînable | no | attested — [MDN, Promise](https://developer.mozilla.org/fr/docs/Web/JavaScript/Reference/Global_Objects/Promise) | Le verbe est `enchaîner` ; accordez `chaînable` au nom décrit. |
| subquery; bare subquery; anonymous CTE | sous-requête; sous-requête sans alias; CTE anonyme | no | attested — [Microsoft, concepteur de requêtes](https://learn.microsoft.com/fr-fr/power-bi/paginated-reports/report-builder/relational-query-designer-user-interface-report-builder) | `CTE` reste l'acronyme SQL ; `WITH` reste du code. |
| join; left join; outer join; null semantics | jointure; jointure à gauche; jointure externe; sémantique des valeurs nulles | no | attested — [ClickHouse en français](https://clickhouse.com/fr/clickhouse), [Microsoft, concepteur de requêtes](https://learn.microsoft.com/fr-fr/power-bi/paginated-reports/report-builder/relational-query-designer-user-interface-report-builder) | Utilisez `jointure`, pas `join`, dans une phrase française ordinaire. Gardez `NULL` uniquement quand le mot-clé SQL est réellement écrit. |
| predicate; filter; where clause; group by; order by; limit | prédicat; filtre; clause `WHERE`; regroupement (`GROUP BY`); tri (`ORDER BY`); limite (`LIMIT`) | no | attested — [Microsoft, concepteur de requêtes](https://learn.microsoft.com/fr-fr/power-bi/paginated-reports/report-builder/relational-query-designer-user-interface-report-builder) | Le concept est français ; les mots-clés SQL entre backticks restent inchangés. |
| projection; selection (select list); alias; expression; window expression | projection; sélection (liste `SELECT`); alias; expression; expression de fenêtre | no | inferred — du vocabulaire SQL français attesté par [Microsoft](https://learn.microsoft.com/fr-fr/power-bi/paginated-reports/report-builder/relational-query-designer-user-interface-report-builder) | `alias` est un nom français ; `liste SELECT` désigne la liste de sélection SQL. |
| aggregate; function; table function; scalar | agrégat; fonction; fonction de table; scalaire | no | attested — [ClickHouse en français](https://clickhouse.com/fr/clickhouse) | Accordez `scalaire` au nom décrit. |
| raw SQL; SQL template; SQL fragment | SQL brut; modèle SQL; fragment SQL | no | inferred — du vocabulaire de [Microsoft SQL](https://learn.microsoft.com/fr-fr/sql/relational-databases/security/sql-injection?view=sql-server-ver17) et de la documentation française | `SQL` reste l'acronyme ; `modèle` traduit le concept de template. |
| parameter; bound parameter; placeholder; query parameter | paramètre; paramètre lié; espace réservé; paramètre de requête | no | attested — [Microsoft, paramètres](https://learn.microsoft.com/fr-fr/SQL/connect/ado-net/configure-parameters?view=azuresqldb-current) | Accord : `un paramètre lié` ; `espace réservé` est la forme française de `placeholder`. |
| compile / compiled SQL | compiler / SQL compilé | no | inferred — du vocabulaire français de TypeScript et du modèle `SQL compilé` | `SQL` est masculin dans `le SQL compilé`. |
| thenable | semi-promesse | no | attested — [MDN, Promise](https://developer.mozilla.org/fr/docs/Web/JavaScript/Reference/Global_Objects/Promise) | MDN emploie `semi-promesse` et mentionne `thenable` seulement comme forme anglaise ; utilisez la forme française. |
| helper; namespace; factory; callback; iterator; variadic | fonction utilitaire; espace de noms; fabrique; fonction de rappel; itérateur; variadique | no | inferred — [MDN, promesses](https://developer.mozilla.org/fr/docs/Web/JavaScript/Guide/Using_promises) et [TypeScript](https://www.typescriptlang.org/fr/docs/handbook/2/everyday-types.html) | Les noms d'API littéraux ne changent pas ; accordez `variadique` au nom féminin qu'il décrit. |
| insert; write; batch; bulk insert; insert from select | insertion (`INSERT`); écriture; lot; insertion en masse; insertion depuis `SELECT` | no | attested — [ClickHouse en français](https://clickhouse.com/fr/clickhouse) | Le concept est français ; `INSERT` et `SELECT` sont des mots-clés SQL. |
| stream / streaming; row; result set; replacement (ReplacingMergeTree); versioning; deletion flag | flux / diffusion en continu; ligne; jeu de résultats; remplacement (`ReplacingMergeTree`); gestion des versions; indicateur de suppression | no | inferred — [Microsoft, flux de données en streaming](https://learn.microsoft.com/fr-fr/power-bi/transform-model/dataflows/dataflows-streaming) et [ClickHouse](https://clickhouse.com/fr/clickhouse) | `ReplacingMergeTree` reste inchangé ; `ligne` et `gestion` sont féminins. |
| client; connection; server; replica | client; connexion; serveur; réplica | no | attested — [MDN, messages HTTP](https://developer.mozilla.org/fr/docs/Web/HTTP/Guides/Messages), [Microsoft, réplica](https://learn.microsoft.com/fr-fr/windows-server/storage/storage-replica/storage-replica-overview) | En français technique, écrivez `un réplica`, avec l'accent. |
| request; response; transport; HTTP request | requête; réponse; transport; requête HTTP | no | attested — [MDN, messages HTTP](https://developer.mozilla.org/fr/docs/Web/HTTP/Guides/Messages) | N'utilisez pas `demande` pour une requête HTTP. `HTTP` reste l'acronyme. |
| timeout; settings; configuration / config; option | délai d'expiration; paramètres; configuration; option | no | attested — [MDN, statut 408](https://developer.mozilla.org/fr/docs/Web/HTTP/Reference/Status/408), [Microsoft, formats](https://learn.microsoft.com/fr-fr/windows/apps/design/globalizing/use-global-ready-formats) | `settings` devient `paramètres` en prose ; un nom de champ `config` reste littéral. |
| session; temporary-table workflow; transaction | session; parcours avec table temporaire; transaction | no | attested — [MDN, session HTTP](https://developer.mozilla.org/fr/docs/Web/HTTP/Guides/Session) | La session ClickHouse n'est pas une transaction ; `un parcours` est masculin. |
| concurrency; lifecycle; cleanup | concurrence; cycle de vie; nettoyage | no | inferred — [OpenTelemetry, traces](https://opentelemetry.io/fr/docs/concepts/signals/traces/) et le vocabulaire français des systèmes concurrents | `concurrence` désigne l'exécution simultanée, pas la compétition commerciale. |
| runtime method; execute; fetch; abort / cancel | méthode d'exécution; exécuter; récupérer; abandonner / annuler | no | inferred — [MDN, Promise](https://developer.mozilla.org/fr/docs/Web/JavaScript/Reference/Global_Objects/Promise) et [MDN, messages HTTP](https://developer.mozilla.org/fr/docs/Web/HTTP/Guides/Messages) | `fetch` reste inchangé seulement comme nom de l'API JavaScript. |
| observability; tracing | observabilité; traçage | no | attested — [OpenTelemetry, introduction](https://opentelemetry.io/fr/docs/concepts/observability-primer/), [Microsoft, suivi distribué .NET](https://learn.microsoft.com/fr-fr/dotnet/core/diagnostics/distributed-tracing) | `observabilité` et `traçage` sont les formes du domaine ; l'introduction OpenTelemetry écrit aussi « traçage distribué » et « observer les requêtes ». Microsoft .NET emploie « suivi distribué » : ne l'adoptez pas. |
| span | span | yes | attested — [OpenTelemetry, traces](https://opentelemetry.io/fr/docs/concepts/signals/traces/), [OpenTelemetry, introduction](https://opentelemetry.io/fr/docs/concepts/observability-primer/) | Terme de l'écosystème OpenTelemetry laissé en anglais ; pluriel usuel `spans`. Formes attestées : « span parent », « spans enfants » et « relation parent-enfant entre un span racine et ses spans enfants ». N'adoptez ni `étendue` (Microsoft, Azure Monitor) ni `segment` (Google, AWS X-Ray) : ces formes viennent de pages traduites automatiquement. |
| logging | journalisation | no | attested — [Microsoft, journalisation .NET](https://learn.microsoft.com/fr-fr/dotnet/core/extensions/logging/overview) | `journalisation` est le nom du domaine. |
| logger | logger | yes | attested — [Microsoft, notions ASP.NET Core](https://learn.microsoft.com/fr-fr/aspnet/core/fundamentals/?view=aspnetcore-3.1) | Terme de composant/API laissé en anglais dans la documentation française ; ne le francisez pas en `journaliseur` dans ce contexte. |
| metrics; instrumentation | métriques; instrumentation | no | attested — [OpenTelemetry, introduction](https://opentelemetry.io/fr/docs/concepts/observability-primer/) | `métrique` est féminin ; `instrumentation` est féminin. |
| plugin | plugin | yes | attested — [Vue, plugins](https://fr.vuejs.org/guide/reusability/plugins.html) | Mot déjà intégré en français technique : la page Vue écrit « plugins » et « un plugin ». Pluriel `plugins`. Évitez `greffon` et `module d'extension` ; l'API reste `plugins`. |
| hook | hook | yes | attested — [React en français](https://fr.react.dev/learn), [Vue, cycle de vie](https://fr.vuejs.org/guide/essentials/lifecycle.html) | Terme d'API/écosystème laissé en anglais : Vue écrit « hooks du cycle de vie ». Ne le traduisez pas par `crochet` : Microsoft (Azure Functions) mélange « hooks » et « crochets ». |
| exporter | exportateur | no | attested — [OpenTelemetry, traces](https://opentelemetry.io/fr/docs/concepts/signals/traces/) | La page écrit « Exportateurs de Traces » ; Microsoft et Google emploient aussi `exportateur`. `exporteur` n'apparaît qu'une fois, sur une page Elastic : ne l'adoptez pas. Masculin : `un exportateur`. |
| in-memory exporter | exportateur en mémoire | no | unverified | Composé de deux formes attestées (`exportateur`, `en mémoire`), non relevé tel quel dans une source. |
| tracer provider; tracer | fournisseur de traceurs; traceur | no | attested — [OpenTelemetry, traces](https://opentelemetry.io/fr/docs/concepts/signals/traces/) | La page écrit « Fournisseur de traceurs » pour `TracerProvider` et « Un Traceur crée des spans ». Le complément `de traceurs` reste au pluriel ; `TracerProvider` reste littéral dans le code. |
| propagator; context propagation | propagateur; propagation du contexte | no | attested — [OpenTelemetry, propagation du contexte](https://opentelemetry.io/fr/docs/concepts/context-propagation/) | Datadog emploie les mêmes formes. Masculin : `un propagateur`. |
| context manager | gestionnaire de contexte | no | unverified — attesté seulement dans le [glossaire Python](https://docs.python.org/fr/3/glossary.html), pas dans la documentation OpenTelemetry | Forme française de base ; `context manager` ne reste en anglais que dans un identifiant. |
| Collector; collector | Collector OpenTelemetry; collecteur | no | attested — [OpenTelemetry, Collector](https://opentelemetry.io/fr/docs/collector/) | « Collector OpenTelemetry » est le nom du produit et reste tel quel ; le nom commun générique est `collecteur` (Datadog, Google). |
| set up (noun / verb) | mise en place / mettre en place | no | attested — [OpenTelemetry, traces](https://opentelemetry.io/fr/docs/concepts/signals/traces/), [OpenTelemetry, Collector](https://opentelemetry.io/fr/docs/collector/) | « mise en place du traçage » et « Mettre en place un Collector ». |
| entry point | point d'entrée | no | attested — [Microsoft, point d'entrée C#](https://learn.microsoft.com/fr-fr/dotnet/csharp/fundamentals/program-structure/main-command-line) | `un point d'entrée` ; le nom de sous-chemin comme `ck-orm/otel` reste littéral. |
| opt-in | à activer explicitement | no | attested — Datadog, page d'ingestion OTLP | N'utilisez pas `facultatif`, qui veut dire « optional » et non « à activer ». |
| environment variable | variable d'environnement | no | attested — Microsoft et Google | Le nom de la variable reste littéral. |
| SDK | SDK | yes | attested — [OpenTelemetry, traces](https://opentelemetry.io/fr/docs/concepts/signals/traces/) | Acronyme laissé tel quel, masculin : `un SDK`. |
| event | événement | no | attested — [OpenTelemetry, traces](https://opentelemetry.io/fr/docs/concepts/signals/traces/) | Accent obligatoire sur `événement`. |
| error; exception; throws / rejects; error model; validation error; stack; retry | erreur; exception; lève / rejette; modèle d'erreur; erreur de validation; pile d'appels; nouvelle tentative | no | attested — [TypeScript](https://www.typescriptlang.org/fr/docs/handbook/2/basic-types.html), [MDN, Promise](https://developer.mozilla.org/fr/docs/Web/JavaScript/Reference/Global_Objects/Promise) | Écrivez `lever` une erreur, jamais `jeter` une erreur ; `lève` et `rejette` s'accordent avec leur sujet. |
| security; SQL injection; untrusted input; escaping | sécurité; injection SQL; entrée non approuvée; échappement | no | attested — [Microsoft, injection SQL](https://learn.microsoft.com/fr-fr/sql/relational-databases/security/sql-injection?view=sql-server-ver17), [MDN, sécurité](https://developer.mozilla.org/fr/docs/Learn_web_development/Extensions/Server-side/First_steps/Website_security) | `SQL` reste fixe ; `entrée non approuvée` est la forme de la source Microsoft. |
| identifier quoting | mise entre guillemets des identifiants | no | unverified | Forme descriptive de base ; faites confirmer `quotage des identifiants` si l'équipe préfère le terme court. |
| credentials; password; allow-list; permission | identifiants d'authentification; mot de passe; liste d'autorisation; autorisation | no | attested — [Microsoft, autorisations](https://learn.microsoft.com/fr-fr/troubleshoot/entra/entra-id/app-integration/application-delegated-permission-access-tokens-identity-platform) | `permission` devient `autorisation` ; distinguez-la des `identifiants` de connexion. |
| installation; install; peer dependency | installation; installer; dépendance entre pairs | no | attested — [pnpm, dépendances entre pairs](https://pnpm.netlify.app/fr/blog/2021/12/29/yearly-update) | `une dépendance entre pairs` ; ne laissez pas `peer dependency` dans la prose. |
| version; pin a version; release; breaking change; before 1.0 / pre-release | version; figer une version; publication; changement incompatible; avant 1.0 / préversion | no | inferred — du vocabulaire de publication logicielle française et de [pnpm](https://pnpm.netlify.app/fr/blog/2021/12/29/yearly-update) | `préversion` est féminin ; gardez `1.0` tel quel comme identifiant de version. |
| migration (schema migration) | migration (migration de schéma) | no | unverified | Forme française de base ; précisez `de schéma` quand il s'agit du schéma de données. |
| production | production | no | attested — [OpenTelemetry, Ops](https://opentelemetry.io/fr/docs/getting-started/ops/) | Même graphie en français ; écrivez `en production`. |
| example; runnable; reference; documentation; page; section | exemple; exécutable; référence; documentation; page; section | no | attested — [Microsoft, style rapide](https://learn.microsoft.com/fr-fr/contribute/content/style-quick-start) | `exécutable` décrit ce que le lecteur peut lancer ; `documentation`, `page` et `section` sont des noms français. |
| caveat; note; behavior; semantics; contract; mode | point d'attention; remarque; comportement; sémantique; contrat; mode | no | attested — [TypeScript](https://www.typescriptlang.org/fr/docs/handbook/2/everyday-types.html), [Microsoft, style rapide](https://learn.microsoft.com/fr-fr/contribute/content/style-quick-start) | `caveat` devient `point d'attention` ; `sémantique` est féminin. |
| public API; stable; internal; deprecated | API publique; stable; interne; déprécié | no | attested — [TypeScript](https://www.typescriptlang.org/fr/docs/handbook/2/basic-types.html) | Accordez `publique`, `interne` et `déprécié` au nom ; `l'API` entraîne l'élision. |
| shape; surface; layer | structure; surface d'API; couche | no | unverified | Formes françaises de base ; choisissez `structure` ou `surface` selon le référent concret. |
| keeps X easy | garde X simple | no | inferred — reformulation active conforme à [Microsoft, style rapide](https://learn.microsoft.com/fr-fr/contribute/content/style-quick-start) | Remplacez X par le nom réel et accordez l'adjectif. |
| without hiding | sans masquer | no | inferred — reformulation active conforme à [Microsoft, style rapide](https://learn.microsoft.com/fr-fr/contribute/content/style-quick-start) | Gardez l'agent implicite seulement si le contexte le rend évident. |
| make everyday access easier to structure | aide à structurer l'accès courant | no | inferred — reformulation active conforme à [Microsoft, style rapide](https://learn.microsoft.com/fr-fr/contribute/content/style-quick-start) | Préférez le verbe concret à une nominalisation. |
| is rejected | est rejeté | no | inferred — construction passive française ; accordez `rejeté` avec le sujet | Le participe varie : `la valeur est rejetée`, `les paramètres sont rejetés`. |
| is not validated | n'est pas validé | no | inferred — construction passive française ; accordez `validé` avec le sujet | Écrivez `la donnée n'est pas validée` au féminin. |
| is decoded conservatively | est décodé avec prudence | no | inferred — reformulation idiomatique française | Accordez `décodé` avec le résultat décrit ; évitez `prudemment` si le contexte porte sur la méthode. |
| carries | transporte | no | inferred — verbe concret choisi selon le contexte de données | Remplacez par `porte` si le sujet est une structure ou une information. |
| exposes | expose | no | inferred — verbe concret du vocabulaire des API françaises | Même graphie qu'en anglais, mais verbe français conjugué. |
| follows | suit | no | inferred — verbe concret du vocabulaire des API françaises | Accordez le sujet ; ne laissez pas `follows` en anglais. |
| stays | reste | no | inferred — verbe concret du vocabulaire des API françaises | Ne pas remplacer par `demeure` dans la prose courante. |
| leaves | laisse | no | inferred — verbe concret du vocabulaire des API françaises | Choisissez `laisse` ou `conserve` selon le sens exact. |
| pass | transmettez | no | inferred — impératif direct conforme au registre de la section 2 | Employez `passez` seulement si le contexte décrit réellement un passage. |
| wrap | encapsuler | no | inferred — vocabulaire français de la programmation | Accord et temps suivent la phrase ; pas de calque `wrapper` dans la prose. |
| compose | composer | no | inferred — vocabulaire français des fonctions | Gardez `compose` uniquement dans un identifiant ou un nom d'API. |
| accepts | accepte | no | inferred — verbe concret du vocabulaire des API françaises | Le sujet commande le nombre : `la fonction accepte`, `les fonctions acceptent`. |
| returns | renvoie | no | attested — [MDN, Promise](https://developer.mozilla.org/fr/docs/Web/JavaScript/Reference/Global_Objects/Promise) | Préférez `renvoie` à `retourne` dans la prose de ce guide. |
| throws | lève | no | attested — [TypeScript](https://www.typescriptlang.org/fr/docs/handbook/2/basic-types.html), [MDN, Promise](https://developer.mozilla.org/fr/docs/Web/JavaScript/Reference/Global_Objects/Promise) | Dans le contexte d'une erreur, le verbe est `lever`. |

### 7. Wording guard

Ces formes déclenchent un avertissement de style dans la prose fr-FR parce
qu'elles marquent une autre variante ou un registre rejeté pour ce projet. Ce
ne sont pas des interdictions lexicales universelles ; la garde ne s'applique
pas aux citations, aux données utilisateur ni au code.

| Do not use | Why | Prefer | Source |
|---|---|---|---|
| `fin de semaine` | Forme attestée au Québec et au Canada, mais l'usage français de France privilégie `week-end`. | `week-end` | Canada : [OQLF, fin de semaine](https://vitrinelinguistique.oqlf.gouv.qc.ca/fiche-gdt/fiche/8361442/fin-de-semaine) ; France : [Larousse, week-end](https://www.larousse.fr/dictionnaires/francais/week-end/82754) |
| `magasiner` | Verbe attesté au Québec et au Canada ; en France, la forme courante est `faire des courses` ou `faire les magasins`. | `faire des courses` ou `faire les magasins` | Canada : [OQLF, magasiner](https://vitrinelinguistique.oqlf.gouv.qc.ca/fiche-gdt/fiche/8357752/magasiner) ; France : [Larousse, magasiner](https://www.larousse.fr/dictionnaires/francais/magasiner/48519) |
| `cellulaire` / `téléphone cellulaire` | Dans ce sens, la forme est marquée Canada/Québec ; le français de France emploie `mobile` ou `téléphone portable`. | `mobile` / `téléphone portable` | Canada : [OQLF, téléphone cellulaire](https://vitrinelinguistique.oqlf.gouv.qc.ca/fiche-gdt/fiche/8393991/telephone-cellulaire) ; France : [Larousse, cellulaire](https://www.larousse.fr/dictionnaires/francais/cellulaire/14022) et [portable](https://www.larousse.fr/dictionnaires/francais/portable/62679) |
| `faire sens` | Calque déconseillé par l'Académie française ; ce n'est pas une différence régionale. | `avoir du sens` ou `prendre son sens` | [Académie française, « faire »](https://www.academie-francaise.fr/faire) |
| `ClicHouse`, `CliqueHouse`, `Click-House`, `Javascript`, `Java Script`, `Typescript`, `Type Script`, `ck ORM`, `CK-ORM` | Graphies ou traductions interdites pour des noms fixes. | `ClickHouse`, `JavaScript`, `TypeScript`, `ck-orm` | [ClickHouse](https://clickhouse.com/), [TypeScript](https://www.typescriptlang.org/) et politique commune du projet |

### 8. Model passage

**Modèle mental**

Gardez ces limites en tête quand vous utilisez `ck-orm` :

- `ckType.*` définit les types de colonnes du schéma
- `ckTable(...)` définit les schémas de table
- `ck.*` est l'espace de noms des fonctions utilitaires de requête
- `fn.*` est l'espace de noms des fonctions utilitaires SQL
- le schéma décrit les tables et les colonnes, pas le nom de la base de données
- la connexion à la base de données passe par `clickhouseClient(...)`
- les requêtes construites sont le choix par défaut ; le SQL brut reste disponible pour les expressions propres à ClickHouse
- `runInSession()` est une fonction utilitaire de session ClickHouse, pas une transaction
- `plugins` observent chaque requête envoyée par ck-orm ; le traçage est le plugin `ck-orm/otel`, à activer explicitement et séparé du point d'entrée principal
- `leftJoin()` et `asofLeftJoin()` utilisent par défaut la sémantique SQL des valeurs nulles
- les résultats numériques volumineux ou à risque sont décodés avec prudence, sans conversion silencieuse

**Installation**

ck-orm n'a aucune dépendance obligatoire. Le traçage OpenTelemetry est un point d'entrée distinct, `ck-orm/otel`, et `@opentelemetry/api` est une dépendance entre pairs facultative que lui seul utilise. Le point d'entrée principal `ck-orm` n'importe jamais OpenTelemetry : une application, un bundle pour navigateur ou un worker edge qui ne trace pas n'installe ni n'embarque rien de tout cela. Pour tracer vos requêtes, suivez la mise en place décrite dans [Plugin OpenTelemetry](#opentelemetry-plugin).

### 9. Sources consulted

- [Microsoft, style rapide fr-FR](https://learn.microsoft.com/fr-fr/contribute/content/style-quick-start) — ton direct, voix active, phrases courtes et mots courants.
- [Microsoft, formatage du texte](https://learn.microsoft.com/fr-fr/contribute/content/text-formatting-guidelines) — code inline, mots-clés, noms de méthodes et formatage technique.
- [Microsoft, référence Markdown](https://learn.microsoft.com/fr-fr/contribute/content/markdown-reference) et [remplacement des guillemets](https://learn.microsoft.com/fr-fr/contribute/content/docs-authoring/smart-quote-replacement) — apostrophe ASCII et contraintes des outils Markdown.
- [Microsoft, contenu d'interface](https://learn.microsoft.com/fr-fr/power-platform/well-architected/experience-optimization/user-interface-content) — libellés courts et orientés tâche.
- [Unicode, U+202F](https://www.unicode.org/L2/L2019/19116-clarify-nnbsp.pdf) et [Unicode, texte français](https://www.unicode.org/L2/L2020/20008-core-text.pdf) — espace fine insécable et usages typographiques français.
- [Académie française, nombres](https://www.academie-francaise.fr/questions-de-langue) et [« faire »](https://www.academie-francaise.fr/faire) — groupement des nombres et rejet de `faire sens`.
- [MDN, messages HTTP](https://developer.mozilla.org/fr/docs/Web/HTTP/Guides/Messages), [MDN, session](https://developer.mozilla.org/fr/docs/Web/HTTP/Guides/Session) et [MDN, Promise](https://developer.mozilla.org/fr/docs/Web/JavaScript/Reference/Global_Objects/Promise) — requête, réponse, session, chaînage et vocabulaire JavaScript.
- [MDN, utiliser les promesses](https://developer.mozilla.org/fr/docs/Web/JavaScript/Guide/Using_promises) — fonction de rappel, composition et chaînage.
- [TypeScript, types courants](https://www.typescriptlang.org/fr/docs/handbook/2/everyday-types.html) et [types objet](https://www.typescriptlang.org/fr/docs/handbook/2/objects.html) — tableaux, tuples, inférence et assertions de type.
- [React en français](https://fr.react.dev/learn), [Vue en français](https://fr.vuejs.org/guide/introduction.html) et [TypeScript](https://www.typescriptlang.org/fr/docs/handbook/2/everyday-types.html) — vocabulaire des composants, du rendu et du typage.
- [Microsoft, concepteur de requêtes](https://learn.microsoft.com/fr-fr/power-bi/paginated-reports/report-builder/relational-query-designer-user-interface-report-builder) et [Yii](https://www.yiiframework.com/doc/guide/2.0/fr/db-query-builder) — requête, jointure et coexistence de plusieurs formes pour `query builder`.
- [ClickHouse en français](https://clickhouse.com/fr/clickhouse) — tables, colonnes, jointures, serveur, insertions et réplicas.
- [OpenTelemetry en français](https://opentelemetry.io/fr/docs/concepts/observability-primer/), [traces](https://opentelemetry.io/fr/docs/concepts/signals/traces/), [propagation du contexte](https://opentelemetry.io/fr/docs/concepts/context-propagation/) et [Collector](https://opentelemetry.io/fr/docs/collector/) — observabilité, traces, spans, exportateurs, fournisseur de traceurs, propagateurs, collecteur, mise en place, métriques et instrumentation.
- [Vue, plugins](https://fr.vuejs.org/guide/reusability/plugins.html) et [cycle de vie](https://fr.vuejs.org/guide/essentials/lifecycle.html) — `plugin`, `hooks du cycle de vie`.
- [Microsoft, point d'entrée C#](https://learn.microsoft.com/fr-fr/dotnet/csharp/fundamentals/program-structure/main-command-line), [Microsoft, suivi distribué .NET](https://learn.microsoft.com/fr-fr/dotnet/core/diagnostics/distributed-tracing) et [glossaire Python](https://docs.python.org/fr/3/glossary.html) — `point d'entrée`, forme `suivi distribué` non retenue, `gestionnaire de contexte`.
- [Microsoft, injection SQL](https://learn.microsoft.com/fr-fr/sql/relational-databases/security/sql-injection?view=sql-server-ver17) et [MDN, sécurité](https://developer.mozilla.org/fr/docs/Learn_web_development/Extensions/Server-side/First_steps/Website_security) — entrée non approuvée, échappement et injection SQL.
- [pnpm, dépendances entre pairs](https://pnpm.netlify.app/fr/blog/2021/12/29/yearly-update) — usage français de `dépendance entre pairs` avec la forme `peer dependency` entre parenthèses.
- [Microsoft, flux de données en streaming](https://learn.microsoft.com/fr-fr/power-bi/transform-model/dataflows/dataflows-streaming) — `flux` et `diffusion en continu`.
- Canada : [OQLF, fin de semaine](https://vitrinelinguistique.oqlf.gouv.qc.ca/fiche-gdt/fiche/8361442/fin-de-semaine), [magasiner](https://vitrinelinguistique.oqlf.gouv.qc.ca/fiche-gdt/fiche/8357752/magasiner) et [cellulaire](https://vitrinelinguistique.oqlf.gouv.qc.ca/fiche-gdt/fiche/8393991/telephone-cellulaire) ; France : [Larousse, week-end](https://www.larousse.fr/dictionnaires/francais/week-end/82754), [magasiner](https://www.larousse.fr/dictionnaires/francais/magasiner/48519), [cellulaire](https://www.larousse.fr/dictionnaires/francais/cellulaire/14022) et [portable](https://www.larousse.fr/dictionnaires/francais/portable/62679) — contraste régional documenté des trois entrées de section 7.

### 10. Open questions

- Faites relire par un développeur français la préférence entre `constructeur de requêtes`, `concepteur de requêtes` et `builder` quand le mot désigne une API.
- Faites confirmer le registre de `fabrique`, `logger`, `mise entre guillemets des identifiants`, `préversion` et `dépendance entre pairs` dans les équipes françaises ciblées.
- Faites vérifier les accords de `inféré`, `typé`, `imbriqué`, `décodé` et `déprécié` sur leurs référents réels ; la forme varie avec le genre et le nombre du nom.
- Vérifiez dans le générateur Markdown que U+202F reste insécable avant `: ; ! ?`, dans `« »` et entre un nombre et une unité ; prévoyez U+00A0 ou `&#8239;` si le pipeline le perd.
- La préférence `14:30` contre `14 h 30` et la règle d'ordinal `1er`/`1re`/`2e` restent à confirmer par une relecture éditoriale française.
