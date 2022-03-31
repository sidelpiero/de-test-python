# de-test-python
python-DE-test

1. Code de géneration du Graph JSON, tout est fait sous PySpark : 
  - Code testé j'arrive à bien génerer le JSON, concernant l'organisation du code, il reste à   decouper les fonctions en utils et les mettre dans le fichier utils.py / repertoire utils
  - le reformattage des dates n'est pas complet, comme amélioration on aura encore à transformer toutes les dates en (YYYY/MM/DD - MM/DD/YYYY ...) vers du YYYY-MM-DD
  - je n'ai pas eu le temps de detailler la mise en place de l'environement local PySpark, mais j'ai bien testé le code est fonctionnel
  - comme amélioration supplémentaire, en vue de passer le code en Prod il serait judicieux de partitionner les données par date et parametrer le code par Date, afin d'avoir une pipeline de traitement au jour le jour (sous composer/airflow et le dag joint au code)
2. pour du code avec une grosse volumétrie quelques points d'attention :
- la restitution du resultat en JSON (conversion du Dataframe) doit se faire par une routine PySpark et non Pandas ralanti le traitement
- partitionnement des données (en date par exemple)
- passer les traitement sur un filesystem plus conséquent (GCS par exemple) et non en local
- faire le traitement directement sur cloud public GCP par exemple avec un cluster DataProc (gcloud dataproc jobs submit spark ...)
3. la Partie SQL est dans le fichier SQL, par contrainte de temps scripting et testing je me suis arréter en milieu de la partie 2 
