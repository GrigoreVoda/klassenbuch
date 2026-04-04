# Klassenbuch PDF zu SQL & Excel

Dieses Projekt extrahiert Daten aus digitalen Klassenbüchern (PDF), speichert diese strukturiert in einer SQL-Datenbank (PostgreSQL) und exportiert sie anschließend in eine Excel-Vorlage (Berichtsheft).

## Voraussetzungen

* **Python:** Version 3.8 oder neuer
* **Datenbank:** PostgreSQL
* **Python-Bibliotheken:** Die benötigten Pakete .

## Projektstruktur

* **`klassenbuch_pdf_parsing.py`**: Skript zum Auslesen der Texte/Daten aus den Klassenbuch-PDFs.
* **`pdf_sort_by_date.py`**: Hilfsskript, um die PDFs chronologisch zu sortieren.
* **`init_db.sql.txt`**: SQL-Befehle zur Initialisierung der Tabellenstruktur in der Datenbank.
* **`sql_in_excel_export.py`**: Skript, das die gespeicherten Daten aus der SQL-Datenbank holt und in die Excel-Datei schreibt.
* **`berichtsheft_template.xlsx`**: Die Excel-Vorlage, die für den Export als Berichtsheft genutzt wird.
* **`config.toml`**: Konfigurationsdatei für Pfade, Datenbankverbindungen oder spezifische Einstellungen.

## Einrichtung und Installation

1. **Repository klonen:**
```
   git clone [https://github.com/GrigoreVoda/klassenbuch.git](https://github.com/GrigoreVoda/klassenbuch.git)
   cd klassenbuch
```

2. **Virtuelle Umgebung erstellen und aktivieren**
```
python -m venv .venv

# Linux / macOS
source .venv/bin/activate

# Windows
.venv\Scripts\activate
```
3. **Abhängigkeiten installieren**
```
pip install -r requirements.txt
```

4. **PostgreSQL-Datenbank initialisieren**
```
# Mit PostgreSQL verbinden und ausführen:
CREATE DATABASE klassenbuch;
\c klassenbuch;
-- Dann den Inhalt von init_db.sql.txt einfügen

```
5. **Projekt konfigurieren.**
Öffnen Sie die Datei config.toml und tragen Sie Ihre Einstellungen ein:
```
paths = ["pfad/zu/deinem/pdf/ordner"]

[database]
host     = "localhost" 
port     = 5432
name     = "klassenbuch"
user     = "postgres"
password = "deinpasswort"
```

## Nutzung

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
# Testlauf (keine Schreibvorgänge in die DB – erst testen):
python klassenbuch_pdf_parsing.py --dry-run

# Vollständiger Durchlauf:
python klassenbuch_pdf_parsing.py
```

**Schritt 4: Berichtsheft generieren**
Führe dieses Skript aus, um das finale Berichtsheft aus der SQL-Datenbank zu generieren:
```bash
python sql_in_excel_export.py
```
Das Skript sql_in_excel_export.py nutzt die im Projekt enthaltene berichtsheft_template.xlsx als Basis. Es erstellt Kopien dieser Vorlage und füllt sie automatisch mit den Daten aus der PostgreSQL-Datenbank (Lehrinhalte, Daten, Stunden) aus. So wird dein digitales Klassenbuch direkt in ein fertiges Excel-Berichtsheft umgewandelt.

**Vollständige Pipeline**
```
source .venv/bin/activate
python pdf_sort_by_date.py
python klassenbuch_pdf_parsing.py
python sql_in_excel_export.py
```

**Entwicklung.**
Um die aktuellen Abhängigkeiten nach der Installation neuer Pakete zu speichern:
```
pip freeze > requirements.txt
```
Um die virtuelle Umgebung zu deaktivieren:
```
deactivate
```








