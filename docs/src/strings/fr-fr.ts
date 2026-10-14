import type { Strings } from "../i18n";

export default {
  site: {
    nav: "Navigation du site",
    examples: "Exemples",
    language: "Langue",
    closeNav: "Fermer la navigation",
    license: "Licence MIT",
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
    items: {
      "decimal-sum": {
        title: "Totaux Decimal exacts",
        summary:
          "Calcule la somme d'une colonne Decimal(20, 5) et décode le total en chaîne, pour qu'il ne passe jamais par un nombre à virgule flottante.",
      },
      "literal-search": {
        title: "Recherche littérale sûre",
        summary:
          'Les valeurs sont transmises sous forme de paramètres de requête typés, sans jamais être concaténées dans le SQL, et ck.contains échappe % et _ pour que "alpha%" corresponde littéralement.',
      },
      "left-join": {
        title: "Jointure à gauche avec NULL",
        summary:
          "leftJoin() envoie join_use_nulls = 1, donc les colonnes sans correspondance du côté droit sont NULL dans ClickHouse et null dans le type de la ligne.",
      },
      "website-analytics": {
        title: "Analytique web",
        summary:
          "Compte les pages vues et le nombre exact de visiteurs distincts par chemin pour un seul site sur un intervalle UTC [start, end).",
        notes: "Utilise des tables temporaires de session ; un même visiteur peut être compté sur plusieurs chemins.",
      },
      "asof-price": {
        title: "Dernière cotation avec ASOF JOIN",
        summary:
          "ASOF LEFT JOIN associe chaque transaction à la cotation la plus récente du même symbole dont l'horodatage est antérieur ou égal à celui de la transaction. Les prix Decimal restent des chaînes et, en l'absence de cotation historique, la valeur renvoyée est null.",
        notes: "Utilise des tables temporaires de session.",
      },
      "dynamic-signals": {
        title: "Filtrage des signaux Map",
        summary:
          "Filtre les signaux évolutifs du modèle dans Map(String, Float64), en vérifiant l'existence de la clé avant d'appliquer un seuil inclusif. Un zéro stocké reste un véritable score.",
        notes: "Utilise des tables temporaires de session.",
      },
      "trace-list": {
        title: "Liste de traces avec filtres et total",
        summary:
          "Construit une seule liste de filtres à partir de critères facultatifs et l'utilise à la fois pour la page LIMIT/OFFSET et pour le nombre total, si bien qu'une page au-delà de la dernière indique toujours le total.",
        notes: "Utilise des tables temporaires de session.",
      },
      "support-tickets": {
        title: "Colonnes typées avec validation à l'exécution",
        summary:
          "Des colonnes Enum8, un type marqué avec $type et une structure JSON avec $type restreignent les types des lignes, et un validateur Standard Schema écrit à la main normalise les adresses électroniques et rejette une adresse invalide avant tout envoi.",
        notes: "Utilise des tables temporaires de session.",
      },
      "bluesky-events": {
        title: "Requêtes sur des charges utiles JSON d'événements",
        summary:
          "En reprenant les requêtes de ClickHouse JSONBench, compte les événements Bluesky par collection via des chemins JSON typés, puis lit les publications avec un chemin dynamique converti et un sous-objet JSON.",
        notes: "Utilise des tables temporaires de session.",
      },
      "cdc-rollup": {
        title: "Agrégation de lignes CDC dans ClickHouse",
        summary:
          "Déduplique les lignes CDC de Postgres avec FINAL et les agrège en chiffre d'affaires quotidien avec INSERT ... SELECT, si bien que seules les lignes finales sont renvoyées à JavaScript.",
        notes: "Utilise des tables temporaires de session.",
      },
      "usage-metering": {
        title: "Mesure d'utilisation",
        summary:
          "Agrège l'utilisation par locataire, utilisateur et minute UTC sur plusieurs lots d'insertion, avec une vue matérialisée déclenchée par l'insertion.",
        notes:
          "Exécutez cet exemple de manière séquentielle dans une base de données jetable avec les autorisations CREATE, SELECT, INSERT et DROP. Utilise des tables persistantes à noms fixes et une vue matérialisée, et ne supprime que les objets qu'il a créés avec succès. L'interruption du processus ou la perte d'une réponse CREATE peut laisser des objets à inspecter manuellement.",
      },
      "request-latency": {
        title: "Percentiles de latence des requêtes",
        summary:
          "Agrège le nombre de requêtes réussies, la latence moyenne pondérée et les percentiles p95/p99 exacts par locataire, opération et minute UTC, avec une vue matérialisée déclenchée par l'insertion.",
        notes:
          "Exécutez cet exemple de manière séquentielle dans une base de données jetable avec les autorisations CREATE, SELECT, INSERT et DROP. Ne supprime que ses tables à noms fixes et sa vue matérialisée créées avec succès ; l'interruption du processus ou la perte d'une réponse CREATE peut laisser des objets. Les états de quantiles exacts conservent tous les échantillons : évaluez donc la consommation mémoire pour les charges de production.",
      },
    },
    tabsLabel: "{title} : code source et SQL",
  },
} satisfies Strings;
