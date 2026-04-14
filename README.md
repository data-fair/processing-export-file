# <img alt="Data FAIR logo" src="https://cdn.jsdelivr.net/gh/data-fair/data-fair@master/ui/public/assets/logo.svg" width="30"> @data-fair/processing-export-file

Plugin [data-fair/processings](https://github.com/data-fair/processings) qui exporte les lignes d'un jeu de données dans un ou plusieurs fichiers attachés en tant que pièces jointes de métadonnées. Prend en charge 7 formats : CSV, Parquet, XLSX, GeoJSON, PMTiles, Shapefile, GeoPackage.

## Features

- **Multi-format** — génère simultanément plusieurs formats sélectionnés (csv, parquet, xlsx, geojson, pmtiles, shp, gpkg).
- **Filtres** — restreint l'export à un sous-ensemble des lignes via filtres `in`, `interval`, `out` (traduits en `qs` via `filters2qs`).
- **Sélection de colonnes** — par défaut exporte toutes les colonnes non-calculées ; configurable par champ.
- **Support géographique** — détecte automatiquement les concepts `latitude` / `longitude` / `geometry` / `latLon` pour générer les formats géo via un VRT OGR intermédiaire.
- **Pagination streamée** — lit le dataset par pages de 10 000 lignes avec backpressure correct, pas de chargement complet en mémoire.
- **Attachement comme métadonnée** — uploade chaque fichier produit sur `/metadata-attachments` du dataset et met à jour les attachments.
- **Graceful stop** — interrompt proprement l'export entre deux pages si la plateforme demande l'arrêt.

## Prérequis système

Les formats **géographiques** dépendent de deux binaires externes :

| Format | Binaire requis |
| --- | --- |
| `geojson`, `shp`, `gpkg` | `ogr2ogr` (paquet `gdal-bin` sur Debian/Ubuntu) |
| `pmtiles` | `tippecanoe` (voir [felt/tippecanoe](https://github.com/felt/tippecanoe)) |

Si un format géo est demandé sans le binaire correspondant, le plugin émet un log d'erreur explicite.

Les formats `csv`, `parquet`, `xlsx` ne nécessitent aucun binaire externe.

## Configuration

### Tab `Jeu de données`

| Champ | Description |
| --- | --- |
| `dataset` | Jeu de données source à exporter |

### Tab `Paramètres`

| Champ | Description |
| --- | --- |
| `fields` | Liste des colonnes à inclure (toutes les non-calculées par défaut) |
| `format` | Un ou plusieurs formats parmi csv/parquet/xlsx/geojson/pmtiles/shp/gpkg |
| `filename` | Nom du fichier sans extension (défaut `export`) |
| `label` | Libellé de la pièce jointe (défaut `Export`) |
| `filters` | Filtres optionnels (valeurs in, interval min/max, valeurs à exclure) |

## Développement

```bash
nvm use            # Node 24
npm install
npm run build-types
npm run lint
npm run test
```

Les tests sont **100% locaux** grâce à [`nock`](https://github.com/nock/nock) qui simule une instance data-fair. Les tests géo (`test-it/run-geo.test.ts`) sont automatiquement skippés si `ogr2ogr` ou `tippecanoe` ne sont pas installés.

Pour tester contre une vraie instance, dupliquer `config/default.mjs` en `config/local-test.mjs` (gitignored) avec une vraie `dataFairUrl` + `dataFairAPIKey`.

## Release

Les plugins sont récupérés depuis npm via le keyword `data-fair-processings-plugin`. Publier équivaut à :

```bash
npm version minor
npm publish
git push --follow-tags
```
