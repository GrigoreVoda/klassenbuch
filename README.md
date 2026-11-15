# Klassenbuch

Es liest die PDFs, legt die Inhalte in einer PostgreSQL-Datenbank ab und 
füllt danach die Excel-Vorlage für das Berichtsheft aus. Pro Kalenderwoche
entsteht eine fertige Excel-Datei.

Das Projekt besteht aus drei kleinen Python-Skripten, die man nacheinander
startet. Wie das geht, steht weiter unten.

## Was man braucht

Python ab Version 3.11. Mit älteren Versionen startet das Skript nicht,
weil es das Modul `tomllib` benutzt.

Eine PostgreSQL-Datenbank. Bei mir läuft sie auf dem gleichen Rechner.

Die PDFs. Sie müssen das Format der GFN-Themendokumentation haben, also
oben Datum und Titel und darunter die Tabelle mit Stunde, Lehrinhalte,
Lernformat und Dozent.

## Einrichten

Das hier macht man nur einmal.

Zuerst das Projekt herunterladen:

```
git clone https://github.com/GrigoreVoda/klassenbuch.git
cd klassenbuch
```

Dann eine eigene Python-Umgebung anlegen und die Pakete installieren:

```
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

Unter Linux oder macOS heißt die zweite Zeile `source .venv/bin/activate`.

Jetzt die Datenbank. In `psql` oder pgAdmin eine Datenbank anlegen:

```
CREATE DATABASE klassenbuch;
```

Danach in diese Datenbank wechseln( \c klassenbuch; )und den ganzen Inhalt der Datei
`init_db.sql.txt` ausführen. Damit entstehen fünf Tabellen: `lernfeld`,
`dozent`, `lernfeld_dozent`, `lerntag` und `unterrichtseinheit`.

Zum Schluss die Datei `config.toml` öffnen und zwei Stellen ausfüllen.
Die Zugangsdaten zur Datenbank:

```
[database]
host     = "localhost"
port     = 5432
name     = "klassenbuch"
user     = "postgres"
password = "deinpasswort"
```

Und den Ordner, in dem die PDFs liegen:

```
[pdfs]
paths = ['C:\Users\Ich\Dokumente\Klassenbuch']
```

Achtung bei Windows-Pfaden: Der Pfad muss in einfachen Anführungszeichen
stehen. Mit doppelten Anführungszeichen bricht das Programm mit der Meldung
`Invalid hex value` ab, weil es den Backslash falsch versteht. Man kann auch
mehrere Ordner eintragen, getrennt mit Komma. Unterordner werden mit
durchsucht.


## Benutzen

### PDFs sortieren

Das ist freiwillig. Die PDFs heißen zum Beispiel
`XX-YYY.JAN_VEACFZZL_29062026_29062026134500.pdf`. Das Datum steht in der
Mitte, deshalb liegen sie im Ordner durcheinander. Das Skript setzt das
Datum nach vorne:

```
python pdf_sort_by_date.py C:\Users\Ich\Dokumente\Klassenbuch
```

Danach heißt die Datei `20260629_MI-YYY.XX_VEACFZZL_29062026_29062026134500.pdf`
und der Ordner ist nach Datum sortiert. Wer erst sehen will, was passieren
würde, hängt `--dry-run` an. Dann wird nichts umbenannt.

Das Skript nur einmal pro Datei laufen lassen. Bei einem zweiten Lauf
bekommen schon umbenannte Dateien das Datum noch einmal davor.

### PDFs einlesen

Ich starte immer zuerst einen Testlauf:

```
python klassenbuch_pdf_parsing.py --dry-run
```

Der Testlauf liest alle PDFs und zeigt die SQL-Befehle an, schreibt aber
nichts in die Datenbank. So sieht man sofort, ob eine PDF Probleme macht.
Wenn alles passt, kommt der echte Lauf:

```
python klassenbuch_pdf_parsing.py
```

Die Ausgabe sieht dann ungefähr so aus:

```
Config: C:\...\klassenbuch\config.toml
PDFs from config: 25 file(s)
Processing: 20260629_MI-YYY.XX_VEACFZZL_29062026_29062026134500.pdf
  ✓ 12 rows affected.
```

Aus jeder PDF kommt ein Lerntag mit neun Unterrichtseinheiten. Steht der
Inhalt einer Stunde in mehreren Zeilen, werden die Zeilen mit einem
Semikolon verbunden, zum Beispiel `Vorgehensmodelle; Scrum; Wasserfallmodell`.

Man kann das Skript so oft starten, wie man will. Tage, die schon in der
Datenbank stehen, werden übersprungen. Ich lasse deshalb einfach alle PDFs
im Ordner liegen und starte das Skript, wenn neue dazugekommen sind.

Soll nur eine einzelne Datei eingelesen werden, schreibt man sie hinter den
Befehl: `python klassenbuch_pdf_parsing.py neue_datei.pdf`

### Berichtsheft erzeugen

```
python sql_in_excel_export.py
```

Das Skript holt alle Tage aus der Datenbank und legt den Ordner
`berichtshefte` an. Darin liegt für jede Kalenderwoche eine Datei, zum
Beispiel `berichtsheft_2026-KW27.xlsx`. Jede Datei ist eine Kopie von
`berichtsheft_template.xlsx`, in die oben die Woche, das Lernfeld und der
Dozent eingetragen sind und darunter für Montag bis Freitag die Inhalte der
einzelnen Stunden.

Die Vorlage muss im gleichen Ordner liegen wie das Skript. Wer ein anderes
Layout braucht, kann die Vorlage in Excel ändern, solange die Zeilen für
die Wochentage an der gleichen Stelle bleiben.

## Wenn etwas nicht geht

`Invalid hex value` beim Start: In der `config.toml` steht ein Windows-Pfad
in doppelten Anführungszeichen. Einfache nehmen.

`Titel / Lernfeld not found in PDF header`: Die Titelzeile der PDF sieht
anders aus als bei den bisherigen. Das ist mir schon zweimal passiert,
einmal ohne Datum im Titel und einmal mit Doppelpunkt hinter dem Lernfeld.
Beides ist inzwischen eingebaut. Bei einem neuen Fall muss das Muster
`TITEL_RE` im Parsing-Skript angepasst werden.

`Nothing to do`: In der `config.toml` ist kein PDF-Ordner eingetragen, oder
der Eintrag steht nicht unter `[pdfs]`.

`permission denied for table ...`: Der Datenbank-Benutzer darf nicht in die
Tabellen schreiben. Das Skript kann die Rechte selbst vergeben, wenn man es
als Administrator der Datenbank startet:
`python klassenbuch_pdf_parsing.py --db-user postgres --fix-permissions --grant-to meinbenutzer`

Alle weiteren Optionen zeigt `python klassenbuch_pdf_parsing.py --help`.

## Die Dateien

- `klassenbuch_pdf_parsing.py` liest die PDFs und schreibt in die Datenbank.
- `sql_in_excel_export.py` erzeugt die Excel-Dateien aus der Datenbank.
- `pdf_sort_by_date.py` benennt die PDFs nach Datum um.
- `config.toml` enthält die Einstellungen.
- `init_db.sql.txt` enthält die Befehle für die Tabellen.
- `berichtsheft_template.xlsx` ist die leere Vorlage.
