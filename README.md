# Weather Project - DE Zoomcamp

### Stack
**VPS**  
| Task                       | Instrument                        |
|----------------------------|-----------------------------------|
| Ingestion + transformation | Bruin (Python asset → SQL assets) |
| Scheduling                 | system cron on VPS                |
| Storage                    | PostgreSQL (stage / core / mart schemas)|
| Admin                      | pgAdmin                           |
| Dashboard                  | Power BI Service (Import mode)    |

### Structure
```
weather-pipeline/
├── Dockerfile
├── docker-compose.yml
├── .env
├── requirements.txt
├── run_pipeline.sh 
├── init-db/
│   └── 01_init_schemas.sql
└── project/
    └── .bruin.yml
```