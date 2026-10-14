import type { Strings } from "../i18n";

export default {
  site: {
    nav: "Sitio",
    examples: "Ejemplos",
    language: "Idioma",
    closeNav: "Cerrar navegación",
    license: "Licencia MIT",
  },
  theme: {
    label: "Tema: {mode}. Cambia a {next}.",
    auto: "Automático",
    light: "Claro",
    dark: "Oscuro",
  },
  code: {
    copy: "Copiar",
    copied: "Copiado",
    copyFailed: "No se pudo copiar",
    variants: "Variantes de código",
    sectionLink: "Enlace a esta sección",
  },
  docs: {
    title: "ck-orm — ClickHouse con seguridad de tipos para TypeScript",
    description:
      "ck-orm es una capa de consultas tipada para ClickHouse en entornos de ejecución de JavaScript modernos: DSL de esquemas, constructor de consultas, plantillas SQL sin formato, sesiones y observabilidad.",
    skip: "Saltar a la referencia",
    tocLabel: "Referencia",
    openNav: "Abrir la navegación de referencia",
    navLabel: "Navegación de referencia",
    badge: "Antes de 0.1.0, los lanzamientos pueden romper la API. Fija una versión exacta.",
    headline: "ClickHouse con seguridad de tipos, sin ocultar ClickHouse",
    intro:
      "ck-orm es un DSL de esquemas, un constructor de consultas tipado y una capa de plantillas SQL para ClickHouse en entornos de ejecución de JavaScript modernos. Las consultas se compilan a SQL de ClickHouse legible, los valores viajan como parámetros tipados y los enteros de 64 bits y los Decimal se decodifican como cadenas exactas.",
    ctaInstall: "Empieza por la instalación",
    ctaExamples: "Ver los ejemplos",
  },
  examples: {
    title: "Ejemplos — ck-orm",
    description: "Ejemplos ejecutables de ck-orm con el SQL exacto que cada uno envía a ClickHouse.",
    skip: "Saltar a los ejemplos",
    tocLabel: "Ejemplos",
    openNav: "Abrir la navegación de ejemplos",
    navLabel: "Navegación de ejemplos",
    heading: "Ejemplos",
    intro:
      "Cada ejemplo es un proyecto ejecutable en {folder}: un {main} y un {package}. Esta página muestra ese mismo archivo. La compilación lo ejecuta mediante ck-orm, así que la pestaña SQL recoge cada solicitud que recibe ClickHouse. El comentario al final de cada archivo muestra las filas que imprime.",
    items: {
      "decimal-sum": {
        title: "Totales Decimal exactos",
        summary:
          "Suma una columna Decimal(20, 5) y decodifica el total como una cadena, así que nunca pasa por un número de coma flotante.",
      },
      "literal-search": {
        title: "Búsqueda literal segura",
        summary:
          'Los valores viajan como parámetros de consulta tipados, nunca se concatenan en el SQL, y ck.contains escapa % y _ para que "alpha%" coincida literalmente.',
      },
      "left-join": {
        title: "Unión izquierda con NULL",
        summary:
          "leftJoin() envía join_use_nulls = 1, así que las columnas del lado derecho sin coincidencia son NULL en ClickHouse y null en el tipo de la fila.",
      },
      "website-analytics": {
        title: "Analítica web",
        summary:
          "Cuenta las páginas vistas y el número exacto de visitantes únicos por ruta para un único sitio en un intervalo UTC [start, end).",
        notes: "Usa tablas temporales de sesión; un mismo visitante puede contarse en varias rutas.",
      },
      "asof-price": {
        title: "Última cotización con ASOF JOIN",
        summary:
          "ASOF LEFT JOIN asocia cada operación con la cotización más reciente del mismo símbolo cuya marca de tiempo sea anterior o igual a la de la operación. Los precios Decimal se mantienen como cadenas y, si no hay una cotización histórica, se devuelve null.",
        notes: "Usa tablas temporales de sesión.",
      },
      "dynamic-signals": {
        title: "Filtrado de señales Map",
        summary:
          "Filtra las señales cambiantes del modelo en Map(String, Float64), comprobando que la clave exista antes de aplicar un umbral inclusivo. Un cero almacenado sigue contando como una puntuación real.",
        notes: "Usa tablas temporales de sesión.",
      },
      "trace-list": {
        title: "Lista de trazas con filtros y total",
        summary:
          "Construye una única lista de filtros a partir de criterios opcionales y la usa tanto para la página con LIMIT/OFFSET como para el recuento total, de modo que una página más allá de la última siga devolviendo el total.",
        notes: "Usa tablas temporales de sesión.",
      },
      "support-tickets": {
        title: "Columnas tipadas con validación en tiempo de ejecución",
        summary:
          "Las columnas Enum8, un tipo marcado con $type y una forma JSON con $type acotan los tipos de las filas, y un validador Standard Schema escrito a mano normaliza las direcciones de correo y rechaza una no válida antes de enviar nada.",
        notes: "Usa tablas temporales de sesión.",
      },
      "bluesky-events": {
        title: "Consultas sobre cargas útiles JSON de eventos",
        summary:
          "Siguiendo las consultas de ClickHouse JSONBench, cuenta los eventos de Bluesky por colección mediante rutas JSON tipadas y después lee las publicaciones con una ruta dinámica convertida a un tipo y un subobjeto JSON.",
        notes: "Usa tablas temporales de sesión.",
      },
      "cdc-rollup": {
        title: "Agregación de filas CDC dentro de ClickHouse",
        summary:
          "Elimina los duplicados de las filas CDC de Postgres con FINAL y las agrega en ingresos diarios con INSERT ... SELECT, de modo que solo las filas finales vuelven a JavaScript.",
        notes: "Usa tablas temporales de sesión.",
      },
      "usage-metering": {
        title: "Medición de uso",
        summary:
          "Agrega el uso por inquilino, usuario y minuto UTC a lo largo de varios lotes de inserción, con una vista materializada desencadenada por inserción.",
        notes:
          "Ejecuta este ejemplo de forma secuencial en una base de datos desechable con permisos CREATE, SELECT, INSERT y DROP. Usa tablas persistentes con nombres fijos y una vista materializada, y solo elimina los objetos que ha creado correctamente. Si el proceso se interrumpe o se pierde la respuesta de CREATE, pueden quedar objetos para su inspección manual.",
      },
      "request-latency": {
        title: "Percentiles de latencia de solicitudes",
        summary:
          "Agrega el número de solicitudes exitosas, la latencia media ponderada y los percentiles p95/p99 exactos por inquilino, operación y minuto UTC, con una vista materializada desencadenada por inserción.",
        notes:
          "Ejecuta este ejemplo de forma secuencial en una base de datos desechable con permisos CREATE, SELECT, INSERT y DROP. Solo elimina las tablas con nombres fijos y la vista materializada que ha creado correctamente; si el proceso se interrumpe o se pierde la respuesta de CREATE, pueden quedar objetos. Los estados de cuantiles exactos conservan todas las muestras, por lo que debes evaluar el uso de memoria para las cargas de trabajo de producción.",
      },
      "query-plugins": {
        title: "Plugins de consulta",
        summary:
          "Un plugin añade una cabecera a cada solicitud y registra, en orden, las sentencias que envía ck-orm, incluidos el CREATE y el DROP de una tabla temporal de sesión.",
        notes: "Usa tablas temporales de sesión.",
      },
      "otel-tracing": {
        title: "Trazado con OpenTelemetry",
        summary:
          "otelTracing() de ck-orm/otel registra un span por cada solicitud. El ejemplo inicia un SDK de OpenTelemetry con un exportador en memoria y después imprime los spans, anidados bajo un span que crea el propio ejemplo.",
        notes: "Usa tablas temporales de sesión.",
      },
    },
    tabsLabel: "{title}: código y SQL",
  },
} satisfies Strings;
