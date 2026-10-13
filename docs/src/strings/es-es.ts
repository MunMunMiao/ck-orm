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
      "Cada ejemplo es un proyecto ejecutable en {folder}: un {main} y un {package}. Esta página muestra ese mismo archivo. La pestaña SQL recoge cada solicitud que envía a ClickHouse, y la prueba unitaria del ejemplo comprueba ese SQL. El comentario al final de cada archivo muestra las filas que imprime.",
    runIntro:
      "Cada ejemplo crea sus propias tablas temporales de sesión, así que funciona con cualquier servidor de ClickHouse:",
    tabsLabel: "{title}: código y SQL",
  },
} satisfies Strings;
