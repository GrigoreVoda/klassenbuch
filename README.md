# Klassenbuch PDF zu SQL & Excel

Dieses Projekt extrahiert Daten aus digitalen Klassenbüchern (PDF), speichert diese strukturiert in einer SQL-Datenbank (PostgreSQL) und exportiert sie anschließend in eine Excel-Vorlage (Berichtsheft).

## 📋 Voraussetzungen

* **Python:** Version 3.8 oder neuer
* **Datenbank:** PostgreSQL
* **Python-Bibliotheken:** Die benötigten Pakete sind in der `requirements.txt` zusammengefasst.

## 📂 Projektstruktur

* **`klassenbuch_pdf_parsing.py`**: Skript zum Auslesen der Texte/Daten aus den Klassenbuch-PDFs.
* **`pdf_sort_by_date.py`**: Hilfsskript, um die PDFs chronologisch zu sortieren.
* **`init_db.sql.txt`**: SQL-Befehle zur Initialisierung der Tabellenstruktur in der Datenbank.
* **`sql_in_excel_export.py`**: Skript, das die gespeicherten Daten aus der SQL-Datenbank holt und in die Excel-Datei schreibt.
* **`berichtsheft_template.xlsx`**: Die Excel-Vorlage, die für den Export als Berichtsheft genutzt wird.
* **`config.toml`**: Konfigurationsdatei für Pfade, Datenbankverbindungen oder spezifische Einstellungen.

## 🚀 Einrichtung und Installation

1. **Repository klonen:**
   ```bash
   git clone [https://github.com/GrigoreVoda/klassenbuch.git](https://github.com/GrigoreVoda/klassenbuch.git)
   cd klassenbuch
   ```

2. **Abhängigkeiten installieren:**


3. **Datenbank initialisieren:**
   Führe die SQL-Befehle aus der Datei `init_db.sql.txt` in deiner PostgreSQL-Datenbank aus.
   1. CREATE DATABASE klassenbuch; ->
   2. \c klassenbuch; ->
   3. Create tables...

## 💻 Nutzung

**Schritt 1: PDFs sortieren (Optional)**
Um die PDFs vorzubereiten und chronologisch zu ordnen:
```bash
python pdf_sort_by_date.py
```

**Schritt 2: Konfiguration ausfüllen**
Öffne die Datei `config.toml` und trage den Pfad zu dem Ordner mit den PDF-Dateien ein:
```toml
paths = ["pfad/zum/ordner/mit/pdf/files"]
```

**Schritt 3: PDFs parsen und in SQL speichern**
Führe das Parsing-Skript aus, um die Daten in die SQL-Datenbank zu schreiben. Zum Testen kannst du das Flag `--dry-run` anhängen:
```bash
# Testlauf (Simulation):
python klassenbuch_pdf_parsing.py --dry-run

# Produktiver Lauf:
python klassenbuch_pdf_parsing.py
```

**Schritt 4: Berichtsheft generieren**
Führe dieses Skript aus, um das finale Berichtsheft aus der SQL-Datenbank zu generieren:
```bash
python sql_in_excel_export.py
```
