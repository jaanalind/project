# Andmeanalüüsi Projekt

## Projekti Ülevaade
See projekt on mõeldud Chicago taksosõitude andmete kogumiseks, töötlemiseks ja analüüsimiseks. Projekt kasutab Apache Airflow'i andmevoogude orkestreerimiseks ja dbt't andmete transformeerimiseks. Andmed salvestatakse PostgreSQL andmebaasi ja on kättesaadavad analüüsi jaoks SQL päringute kaudu.

## Projekti Arhitektuur

### Komponendid
- **Orkestreerimine**: Apache Airflow DAG-id andmevoogude haldamiseks
  - Orkestreeritud on kõik tööd alates andmete sisselugemisest, kuni dbt rakendamiseni
- **Andmete valideerimine** Kasutatud on Pythoni teeki "Pandera". Määratud sobivad vahemikud andmeväljade jaoks,
kui väärtus on lubatud piiridest väljas, siis ride eemaldatakse
- **Andmete Töötlemine**:
  - osa 1: Polars andmete sisselugemiseks ja andmebaasi sisestamiseks ning Pandera andmeraami valideerimiseks
  - osa 2: dbt mudelid andmete transformeerimiseks
- **Andmesalvestus**: PostgreSQL andmebaas (partitsioneeritud tabelid)

### Andmevoog
1. Airflow DAG käivitab iga nädal:
   - Loob uue nädala partitsiooni PostgreSQL-is
   - Kopeerib uue nädala andmed raw_data kausta
   - Valideerib ja laadib andmed andmebaasi
   - Käivitab dbt transformatsioonid

2. dbt transformeerib andmed:
   - Staging: Puhastab andmed (eemaldab null-väärtused, teeb andmetüüpide teisendused)
   - Intermediate: Arvutab juhi vahetused (identifitseerib vahetused 60-minutilise pausi põhjal)
   - Marts: Loob vahetuste statistika (vahetuse kestus, sõitude arv, kogukaugus, kogutulu)

## Seadistamine ja Käivitamine

### Eeldused
- Docker ja Docker Compose
- Conda või Miniconda
- Git

### Keskkonna Seadistamine

1. Klooni repositoorium:
```bash
git clone [repositooriumi-url]
cd [projekti-kaust]
```

2. Loo Conda keskkond:
```bash
conda env create -f environment.yml
conda activate taxi_trip_processor
```

3. Käivita Docker Compose:
```bash
docker-compose up --build
```

Selles punktis peaks rakendus edukalt töötama. 
Andmebaasi connection string on "postgresql://admin:admin@localhost:5433/postgres". Andmebaasis public schema all on
tabelid, kuhu ilmuvad rakenduse tulemused. 
- **driver_shifts** on 2. osa lõplik tulemuste tabel
- **taxi_rides** on *partitioned table*, kuhu ilmuvad kõik andmed csv failidest puhastatud kujul
- **processed_files** hoiab metaandmeid juba töödeldud csv failide kohta 

Kui midagi läks valesti, siis Airflow webserverile saab ligi localhost:8080 username: admin password: admin. Airflow taskid õnnestuvad ainult 2024
alguse jaoks, sest täpselt nii palju on Kaggle'st pärit andmeid, hilisemad taskid failivad.


## Hinnang Lahendusele

Arvan, et lahendus üleüldiselt töötab hästi. Lahenduse tööde õnnestumised on jälgitavad läbi Airflow kasutajaliidese.
Läbi kasutajaliidese saab ka kõiki taske uuesti jooksutada, ilma, et tekiks duplikaatandmeid või rakendus läheks muul viisil katki.

Minu lahenduse pudelikaelad:
  * Hetkel peavad mahtuma ühe nädala andmed mälusse, mis antud andmete puhul ei ole probleemiks.
Kui andmed enam mälusse ei mahuks, oleks võimalik lihtsate sammude abil lugeda andmeid csv failist väiksemate
batchidena ja siis andmed andmebaasi sisestada.
  * Kui andmeid oleks tohutult rohkem, siis oleks väärt kaaluda Apache Sparki kasutamist andmete töötlemiseks mitmete
arvutite peal korraga. Apache Sparki oleks Airflowga hea kasutada ning seejuures teised taskid jäävad puutumata. 

Lahendus võiks sisaldada teste. Antud rakendusel oleks suurem olulisus integratsioonitestidel, sest
mitmed komponendid suhtlevad üksteisega ning kui seal midagi valesti läheb, siis kannatab lõpptulem. Ühikteste vajaks
siiski Pandera validatsioon.

Lahenduses puudub võimalus paroolide korrektseks ja turvaliseks hoiustamiseks. Kui oleks rohkem aega olnud, siis oleks saanud
andmebaasi credentiale hoida nt Airflow secretites. Erinevate Pythoni rakenduste puhul olen kasutanud ka tihti Pydantic-settings teeki.
Hetkel on kahjuks kõik credentialid hard-coded ja pole lihtsat viisi, et rakendus korrektseks muuta.

Airflow peaks olema pigem orkestreerija kui jooksutaja ja oleks ideaalne, kui taskide kood jookseks eraldi venv-is.
Samuti saaksid taskid joosta teistel masinatel näiteks Celery executory abil.
