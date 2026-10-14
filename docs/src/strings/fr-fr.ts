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
      "Chaque exemple est un projet exécutable dans {folder} : un {main} et un {package}. Cette page affiche ce même fichier. La génération du site l'exécute avec ck-orm ; l'onglet SQL liste ainsi chaque requête reçue par ClickHouse. Le commentaire de fin de fichier indique les lignes affichées.",
    details: {
      "website-analytics":
        "Compte les pages vues et le nombre exact de visiteurs distincts par chemin pour un seul site sur un intervalle UTC [start, end). Utilise des tables temporaires de session ; un même visiteur peut être compté sur plusieurs chemins.",
      "usage-metering":
        "Agrège l'utilisation par locataire, utilisateur et minute UTC sur plusieurs lots d'insertion. Exécutez cet exemple de manière séquentielle dans une base de données jetable avec les autorisations CREATE, SELECT, INSERT et DROP. Utilise des tables persistantes à noms fixes et une vue matérialisée, et ne supprime que les objets qu'il a créés avec succès. L'interruption du processus ou la perte d'une réponse CREATE peut laisser des objets à inspecter manuellement.",
      "request-latency":
        "Agrège le nombre de requêtes réussies, la latence moyenne pondérée et les percentiles p95/p99 exacts par locataire, opération et minute UTC. Exécutez cet exemple de manière séquentielle dans une base de données jetable avec les autorisations CREATE, SELECT, INSERT et DROP. Ne supprime que ses tables à noms fixes et sa vue matérialisée créées avec succès ; l'interruption du processus ou la perte d'une réponse CREATE peut laisser des objets. Les états de quantiles exacts conservent tous les échantillons : évaluez donc la consommation mémoire pour les charges de production.",
      "asof-price":
        "ASOF LEFT JOIN associe chaque transaction à la cotation la plus récente du même symbole dont l'horodatage est antérieur ou égal à celui de la transaction. Les prix Decimal restent des chaînes et, en l'absence de cotation historique, la valeur renvoyée est null. Utilise des tables temporaires de session.",
      "dynamic-signals":
        "Filtre les signaux évolutifs du modèle dans Map(String, Float64), en vérifiant l'existence de la clé avant d'appliquer un seuil inclusif. Un zéro stocké reste un véritable score. Utilise des tables temporaires de session.",
    },
    tabsLabel: "{title} : code source et SQL",
  },
} satisfies Strings;
