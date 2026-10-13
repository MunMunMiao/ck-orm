import type { Strings } from "../i18n";

export default {
  site: {
    nav: "Navigation du site",
    examples: "Exemples",
    language: "Langue",
    closeNav: "Fermer la navigation",
    license: "Licence MIT",
    editPage: "Modifier cette page sur GitHub",
    llmsFooter: "llms.txt pour les agents d'IA",
  },
  theme: {
    label: "Thème : {mode}. Passer à {next}.",
    auto: "Automatique",
    light: "Clair",
    dark: "Sombre",
  },
  code: {
    copy: "Copier",
    copied: "Copié",
    copyFailed: "La copie a échoué",
    variants: "Variantes de code",
    sectionLink: "Lien vers cette section",
  },
  docs: {
    title: "ck-orm — ClickHouse typé pour TypeScript",
    description:
      "ck-orm est une couche de requêtes ClickHouse typée pour les environnements d'exécution JavaScript modernes : DSL de schéma, constructeur de requêtes, modèles SQL, sessions et observabilité.",
    skip: "Aller directement à la référence",
    tocLabel: "Référence",
    openNav: "Ouvrir la navigation de la référence",
    navLabel: "Navigation de la référence",
    badge: "Avant 0.1.0, une publication peut casser l'API. Figez une version exacte.",
    headline: "ClickHouse typé, sans masquer ClickHouse",
    intro:
      "ck-orm associe un DSL de schéma, un constructeur de requêtes typé et une couche de modèles SQL pour ClickHouse sur les environnements d'exécution JavaScript modernes. Les requêtes sont compilées en SQL ClickHouse lisible, les valeurs sont transmises sous forme de paramètres typés, et les entiers de 64 bits ainsi que les valeurs Decimal sont décodés en chaînes exactes.",
    ctaInstall: "Commencer par l'installation",
    ctaExamples: "Voir les exemples",
  },
  examples: {
    title: "Exemples — ck-orm",
    description: "Exemples ck-orm exécutables, avec le SQL exact envoyé à ClickHouse.",
    skip: "Aller directement aux exemples",
    tocLabel: "Exemples",
    openNav: "Ouvrir la navigation des exemples",
    navLabel: "Navigation des exemples",
    heading: "Exemples",
    intro:
      "Chaque exemple est un projet exécutable dans {folder} : un {main} et un {package}. Cette page affiche ce même fichier. L'onglet SQL liste chaque requête envoyée à ClickHouse ; le test unitaire de l'exemple vérifie ce SQL. Le commentaire de fin de fichier indique les lignes affichées.",
    runIntro:
      "Chaque exemple crée ses propres tables temporaires de session et s'exécute donc sur n'importe quel serveur ClickHouse :",
    tabsLabel: "{title} : code source et SQL",
  },
} satisfies Strings;
