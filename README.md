1. docker compose up -d -> ay lanci kolchi flow nifi kayn, grafana fih les visualisation li wselt lihom , spark fih deja les script d'ingestion.  
2. docker logs -f nifi -> nifi ki t3etel diro had commande bach tchofo wach dkchi tlanca  
3. docker logs -f spark -> bach t3rfo aussi wach  script f spark wajdin  
4. cd backfill  
5. docker run -d --name backfill-job --network projetbigdata_gdelt-net -e DAYS_BACK=5 gdelt-backfill -> had commande bach diro backfill dial les données 3la hsab chhal derto f variable DAYS_BACK f commande 
