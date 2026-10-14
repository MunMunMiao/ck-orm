import type { Strings } from "../i18n";

export default {
  site: {
    nav: "Sitio",
    examples: "Ejemplos",
    language: "Idioma",
    closeNav: "Cerrar navegación",
    license: "Licencia MIT",
    editPage: "Editar esta página en GitHub",
    llmsFooter: "llms.txt para agentes de IA",
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
    details: {
      "website-analytics":
        "Cuenta las páginas vistas y el número exacto de visitantes únicos por ruta para un único sitio en un intervalo UTC [start, end). Usa tablas temporales de sesión; un mismo visitante puede contarse en varias rutas.",
      "usage-metering":
        "Agrega el uso por tenant, usuario y minuto UTC a lo largo de varios lotes de inserción. Ejecuta este ejemplo de forma secuencial en una base de datos desechable con permisos CREATE, SELECT, INSERT y DROP. Usa tablas persistentes con nombres fijos y una vista materializada, y solo elimina los objetos que ha creado correctamente. Si el proceso se interrumpe o se pierde la respuesta de CREATE, pueden quedar objetos que deban inspeccionarse manualmente.",
      "request-latency":
        "Agrega el número de solicitudes exitosas, la latencia media ponderada y los percentiles p95/p99 exactos por tenant, operación y minuto UTC. Ejecuta este ejemplo de forma secuencial en una base de datos desechable con permisos CREATE, SELECT, INSERT y DROP. Solo elimina las tablas con nombres fijos y la vista materializada que ha creado correctamente; si el proceso se interrumpe o se pierde la respuesta de CREATE, pueden quedar objetos. Los estados de cuantiles exactos conservan todas las muestras, por lo que debes evaluar el uso de memoria para las cargas de trabajo de producción.",
      "asof-price":
        "ASOF LEFT JOIN asocia cada operación con la cotización más reciente del mismo símbolo cuya marca de tiempo sea anterior o igual a la de la operación. Los precios Decimal se mantienen como cadenas y, si no hay una cotización histórica, se devuelve null. Usa tablas temporales de sesión.",
      "dynamic-signals":
        "Filtra las señales cambiantes del modelo en Map(String, Float64), comprobando que la clave exista antes de aplicar un umbral inclusivo. Un cero almacenado sigue contando como una puntuación real. Usa tablas temporales de sesión.",
    },
    tabsLabel: "{title}: código y SQL",
  },
} satisfies Strings;
