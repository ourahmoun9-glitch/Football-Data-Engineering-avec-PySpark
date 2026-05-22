# ⚽ Football Data Engineering avec PySpark
### Statistiques, KPIs et Champions — Bundesliga 2000–2015

![Python](https://img.shields.io/badge/Python-3.8+-blue?logo=python)
![PySpark](https://img.shields.io/badge/PySpark-3.x-orange?logo=apachespark)
![Power BI](https://img.shields.io/badge/PowerBI-Dashboard-yellow?logo=powerbi)
![Status](https://img.shields.io/badge/Status-Completed-brightgreen)

---

## 📌 Description du projet

Ce projet construit un **pipeline de data engineering complet avec PySpark** pour analyser les performances des équipes de football en Bundesliga (Division 1) sur les saisons 2000 à 2015.

Le pipeline couvre toute la chaîne de valeur de la donnée : **chargement → nettoyage → transformation → agrégation → sauvegarde Parquet → visualisation → Power BI**.

---

## 🎯 Objectifs

- Charger et préparer des données brutes de matchs de football
- Calculer des **statistiques avancées** par équipe et par saison (Win%, GoalDifferentials, GoalsPerGame…)
- Identifier les **champions de chaque saison** via des Window Functions
- Stocker les résultats en **format Parquet partitionné**
- Visualiser les performances avec **Matplotlib** et **Power BI**

---

## 👤 User Stories

| Rôle | Besoin |
|---|---|
| **Analyste** | Tableau clair des performances par équipe et par saison |
| **Supporter** | Connaître les champions de chaque saison |
| **Manager sportif** | Indicateurs clés (Win%, buts marqués, GoalDiff) pour décider |

---

## 📁 Structure du projet

```
football-pyspark/
│
├── football_pipeline.py              # Pipeline PySpark complet
├── ensemble-de-donnees.csv           # Dataset source (matchs Bundesliga)
│
├── outputs/
│   ├── football_stats_partitioned/   # Parquet partitionné par saison (toutes équipes)
│   ├── football_top_teams/           # Parquet champions uniquement
│   └── bundesliga_champions_dashboard.png  # Visualisation matplotlib
│
└── README.md
```

---

## 🗂️ Dataset

**Fichier :** `ensemble-de-donnees.csv`  
**Taille :** ~24 625 lignes

| Colonne | Description |
|---|---|
| `Match_ID` | Identifiant unique du match |
| `Div` | Division (`D1` = Bundesliga 1, `D2` = Bundesliga 2) |
| `Season` | Année de la saison |
| `Date` | Date du match |
| `HomeTeam` | Équipe à domicile |
| `AwayTeam` | Équipe à l'extérieur |
| `FTHG` | Full Time Home Goals (buts domicile) |
| `FTAG` | Full Time Away Goals (buts extérieur) |
| `FTR` | Full Time Result (`H`=Home, `A`=Away, `D`=Draw) |

---

## 🔧 Prérequis & Installation

### Dépendances

```bash
pip install pyspark pandas matplotlib
```

### Environnement recommandé

- Python 3.8+
- PySpark 3.x
- Google Colab ou Databricks (Community Edition)
- Power BI Desktop (pour la partie visualisation BI)

> 💡 **Conseil :** PySpark fonctionne très bien sur Google Colab, ce qui facilite l'installation et le traitement de datasets volumineux.

---

## 🚀 Lancer le pipeline

```bash
python football_pipeline.py
```

---

## 🏗️ Pipeline — Étapes détaillées

### 1. Chargement & Préparation des données
- Lecture du CSV dans un DataFrame PySpark
- Renommage des colonnes (`FTHG → HomeGoals`, `FTAG → AwayGoals`, `FTR → Result`)
- Casting des types (Integer, String)

### 2. Création de colonnes indicatrices
Ajout de trois colonnes binaires (0/1) via `F.when()` :

```python
HomeTeamWin  → 1 si Result == "H"
AwayTeamWin  → 1 si Result == "A"
GameTie      → 1 si Result == "D"
```

### 3. Filtrage des données
- Division : `Div == "D1"` (Bundesliga uniquement)
- Saisons : `2000 ≤ Season ≤ 2015`
- **Résultat : 4 896 matchs retenus**

### 4. Agrégations avec Group By
Deux DataFrames créés par `groupBy(Season, Team)` :

- `df_home_matches` — statistiques **à domicile** (wins, goals, draws…)
- `df_away_matches` — statistiques **à l'extérieur**

### 5. Jointure des DataFrames
```python
df_merged = df_home.join(df_away, on=["Season", "Team"], how="inner")
```

### 6. Colonnes synthétiques & KPIs avancés

| Colonne | Calcul |
|---|---|
| `Win` | HomeWins + AwayWins |
| `GoalsScored` | HomeGoalsScored + AwayGoalsScored |
| `GoalDifferentials` | GoalsScored − GoalsAgainst |
| `WinPercentage` | Win / TotalMatches × 100 |
| `GoalsPerGame` | GoalsScored / TotalMatches |
| `GoalsAgainstPerGame` | GoalsAgainst / TotalMatches |

### 7. Classement avec Window Functions
```python
window_season = Window.partitionBy("Season") \
                      .orderBy(col("WinPercentage").desc(),
                               col("GoalDifferentials").desc())

df_ranked = df_stats.withColumn("TeamPosition", rank().over(window_season))
```

### 8. Extraction des champions & sauvegarde Parquet
```python
# Champions (TeamPosition == 1)
df_champions = df_ranked.filter(col("TeamPosition") == 1)

# Sauvegarde
df_ranked.write.partitionBy("Season").parquet("football_stats_partitioned/")
df_champions.write.parquet("football_top_teams/")
```

### 9. Visualisation (Matplotlib)
Trois graphiques générés automatiquement :
- **Win% des champions** par saison (bar chart)
- **Buts marqués** par les champions (bar chart coloré)
- **GoalDifferentials** par saison (bar chart horizontal)

### 10. Power BI Dashboard
Chargement des fichiers Parquet dans Power BI Desktop :

- `football_stats_partitioned` → table `football_stats_all`
- `football_top_teams` → table `football_top_teams`
- Relation : `Season ↔ Season` (many-to-one)

**Visuels recommandés :**
- KPI cards (Win%, record buts, GoalDiff)
- Line chart — évolution Win% des champions
- Donut — répartition buts domicile/extérieur
- Scatter — GoalsScored vs GoalDifferentials
- Table palmarès avec mise en forme conditionnelle

---

## 🏆 Résultats — Champions Bundesliga 2000–2015

| Saison | Champion | Win% | GoalDiff |
|---|---|---|---|
| 2000 | Bayern Munich | 55.9% | +25 |
| 2001 | Leverkusen | 61.8% | +39 |
| 2002 | Bayern Munich | 67.6% | +45 |
| 2003 | Werder Bremen | 64.7% | +47 |
| 2006 | Stuttgart | 58.8% | +28 |
| 2008 | Wolfsburg | 64.7% | +48 |
| 2010 | Dortmund | 67.6% | +45 |
| 2011 | Dortmund | 73.5% | +55 |
| **2012** | **Bayern Munich** | **85.3%** | **+80** ⭐ |
| **2013** | **Bayern Munich** | **85.3%** | **+73** |
| 2015 | Bayern Munich | 82.4% | +64 |

> ⭐ **Record absolu :** Bayern Munich 2012 — 85.3% de victoires et +80 de GoalDifferential

---

## 📊 Compétences démontrées

- **PySpark** — DataFrame API, transformations, agrégations, Window Functions
- **Data Engineering** — pipeline bout-en-bout, partitionnement Parquet
- **Data Analysis** — calcul de KPIs métier avancés
- **Data Visualization** — Matplotlib (Python) + Power BI
- **Planification** — découpage en étapes, user stories, démarche inductive

---

## 👨‍💻 Auteur

**Saad Ourahmoun** — Formation Data Analyst  
Projet réalisé dans le cadre du brief *"Football Data Engineering avec PySpark"*  
Plateforme : [Simplonline](https://maghreb.simplonline.co)

---

## 📄 Licence

Ce projet est réalisé dans un cadre pédagogique.  
Dataset source : football-data.co.uk (open data).
