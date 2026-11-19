# DevOps Metrics Analytics

Proyecto final de **Gestión de Datos e IA**. Pipeline de datos de extremo a extremo que integra fuentes heterogéneas de operaciones DevOps (despliegues, incidentes, monitoreo y tickets de soporte), las limpia, las gobierna, las analiza con Spark, las visualiza en Power BI, genera reportes automatizados y entrena modelos predictivos de aprendizaje automático.

- **Periodo analizado:** 01/01/2026 – 30/06/2026
- **Alcance:** 25 empresas, 6 áreas de producto (Analytics, Auth, Billing, Mobile, Notifications, Data Pipeline)
- **Stack:** Python 3.10+, pandas, PySpark, scikit-learn, Jinja2, WeasyPrint, python-docx, Power BI, GitHub Actions

---

## Tabla de contenidos

1. [Arquitectura del pipeline](#1-arquitectura-del-pipeline)
2. [Estructura del repositorio](#2-estructura-del-repositorio)
3. [Fuentes de datos y esquemas](#3-fuentes-de-datos-y-esquemas)
4. [Descripción de los notebooks](#4-descripción-de-los-notebooks)
5. [Modelado predictivo](#5-modelado-predictivo)
6. [Gobernanza y privacidad](#6-gobernanza-y-privacidad)
7. [Reportería automatizada](#7-reportería-automatizada)
8. [KPIs del periodo](#8-kpis-del-periodo)
9. [Requisitos y reproducción](#9-requisitos-y-reproducción)
10. [Limitaciones conocidas](#10-limitaciones-conocidas)
11. [Equipo](#11-equipo)

---

## 1. Arquitectura del pipeline

```
 FUENTES CRUDAS                 INTEGRACIÓN / LIMPIEZA              CAPA ANALÍTICA
┌──────────────────┐           ┌──────────────────────┐           ┌────────────────────┐
│ devops.db (SQLite)│──┐        │ Guía 1: merge por    │           │ Guía 3: Parquet +  │
│ performance_      │  ├──────▶ │ company_id +         │──────────▶│ PySpark (DataFrame │
│  metrics.json     │  │        │ product_area +       │           │ API y Spark SQL)   │
│ Support_tickets   │──┘        │ event_date           │           └─────────┬──────────┘
│  .csv             │           │ Guía 2/5: limpieza,  │                     │
└──────────────────┘           │ nulos, features de   │                     ▼
                               │ calendario           │           ┌────────────────────┐
                               └──────────────────────┘           │ Guía 4: gobernanza │
                                                                  │ (seudonimización + │
                                                                  │ minimización)      │
                                                                  └─────────┬──────────┘
        ┌─────────────────────────────┬───────────────────────────────────┼─────────────────┐
        ▼                             ▼                                   ▼                 ▼
┌────────────────┐          ┌──────────────────┐               ┌──────────────────┐ ┌───────────────┐
│ Guía 6: capa   │          │ Guía 7: reportes │               │ Guías 8–10: ML   │ │ Power BI      │
│ geográfica     │          │ PDF/DOCX/HTML    │               │ (baseline → RF → │ │ (.pbix)       │
│ (sintética)    │          │ + GitHub Actions │               │ GridSearchCV)    │ │               │
└────────────────┘          └──────────────────┘               └──────────────────┘ └───────────────┘
```

---

## 2. Estructura del repositorio

Todo el contenido vive bajo `content/` (la ruta base usada por los notebooks en Google Colab).

```
content/
├── .github/workflows/
│   └── generar_reporte_devops.yml     # CI: reporte semanal automático
├── notebooks/                         # Un notebook por guía (01 a 10)
├── datos/
│   ├── crudos/                        # devops.db, performance_metrics.json, Support_tickets.csv
│   ├── procesados/                    # datasets limpios e integrados, predicciones
│   ├── analiticos/                    # Parquet (Snappy) generado con Spark
│   ├── geo/                           # datasets con dimensión geográfica
│   ├── gobernanza/                    # dataset publicable (seudonimizado)
│   └── externos/
├── Modelos/                           # Modelos serializados (.joblib)
├── documentos/                        # Política de gobernanza, matriz de riesgos,
│   └── evidencias/                    # métricas de modelos, resultados de GridSearch
├── evidencias/
│   ├── Geo/                           # Capturas de mapas
│   └── reporte/                       # Gráficos usados en el reporte gerencial
├── visualizaciones/
│   └── devopss.pbix                   # Dashboard de Power BI
└── exportaciones/                     # Reportes y metadatos generados
```

---

## 3. Fuentes de datos y esquemas

### 3.1 Fuentes crudas (`datos/crudos/`)

| Archivo | Formato | Contenido | Volumen |
|---|---|---|---|
| `devops.db` | SQLite | Tablas `teams`, `deployments`, `incidents` | 140 equipos · 15,000 despliegues · 4,000 incidentes |
| `performance_metrics.json` | JSON | Métricas de monitoreo (CPU, memoria, latencia, error rate, disponibilidad, RPM) | — |
| `Support_tickets.csv` | CSV | Tickets de soporte con 33 atributos (prioridad, tier, región, sentimiento, impacto, etc.) | 50,000 tickets |

**Esquema de `devops.db`:**

| Tabla | Columnas |
|---|---|
| `teams` | `team_id`, `team_name`, `company_id`, `product_area` |
| `deployments` | `deployment_id`, `team_id`, `company_id`, `product_area`, `event_date`, `environment`, `status`, `lead_time_hours`, `rollback_flag` |
| `incidents` | columnas de `deployments` + `incident_id`, `severity`, `resolution_time_hours`, `downtime_min`, `root_cause` |

### 3.2 Llave de integración

Las tres fuentes se unen por la llave compuesta:

```
company_id + product_area + event_date
```

### 3.3 Dataset integrado y limpio (`datos/procesados/dataset_limpio.csv`)

Granularidad: una fila por empresa, área de producto y día (~20,144 registros).

| Grupo | Columnas |
|---|---|
| Llaves | `company_id`, `product_area`, `event_date` |
| Despliegues | `deployment_count`, `failed_deployments`, `avg_lead_time_hours`, `rollback_count`, `deployment_failure_rate_pct`, `rollback_rate_pct` |
| Incidentes | `incident_count`, `avg_resolution_time_hours`, `total_downtime_min` |
| Soporte | `ticket_count` |
| Monitoreo | `avg_cpu_usage_pct`, `avg_memory_usage_pct`, `avg_response_time_ms`, `avg_error_rate_pct`, `avg_availability_pct`, `avg_requests_per_minute` |
| Derivadas de calendario | `anio`, `mes`, `dia`, `dia_semana`, `es_fin_semana` |
| Calidad | `sin_datos_monitoreo` (bandera de ausencia de telemetría) |

### 3.4 Otros datasets derivados

| Archivo | Descripción |
|---|---|
| `datos/analiticos/devops_metrics_parquet_equipo/` | Dataset analítico en Parquet (Snappy), generado con Spark |
| `datos/geo/dataset_devops_geografico*.csv` | Dataset limpio + `departamento`, `municipio`, `latitud`, `longitud`, `tipo_ubicacion` |
| `datos/gobernanza/dataset_publicable_*.csv` | Vista protegida: `company_token` en lugar de `company_id`, sin `total_downtime_min` |
| `datos/procesados/dataset_power_bi.csv` | Dataset con nulos imputados, listo para Power BI |

---

## 4. Descripción de los notebooks

| Notebook | Guía | Objetivo | Tecnologías clave |
|---|---|---|---|
| `…ProyectoFinal01` | 1 | Ingesta de SQLite, JSON y CSV; verificación de llaves; consolidación de fuentes heterogéneas | pandas, sqlite3, json |
| `…ProyectoFinal02` | 2 | Limpieza, tratamiento de nulos, features de calendario, reporte de calidad | pandas, numpy |
| `…ProyectoFinal03` | 3 | Procesamiento distribuido: DataFrame API vs. Spark SQL, comparación de rendimiento vs. pandas, exportación a Parquet | PySpark |
| `…ProyectoFInal04` | 4 | Gobernanza: clasificación de datos, seudonimización de `company_id`, minimización de `total_downtime_min`, matriz de riesgos | hashlib (SHA-256), PySpark |
| `…ProyectoFinal05` | 5 | Preparación del dataset para Power BI (imputación de nulos) | pandas |
| `…Metrycs…ProyectoFinal06` | 6 | Dimensión geográfica sintética por empresa y validación de coordenadas | pandas |
| `…ProyectoFinal07` | 7 | Reportería automatizada: gráficos, plantillas, exportación a PDF, DOCX y HTML, metadatos | matplotlib, Jinja2, python-docx |
| `…ProyectoFinal08` | 8 | Primer modelo predictivo (incidentes diarios) con baseline y regresión lineal | scikit-learn |
| `…ProyectoFinal09` | 9 | Comparación de modelos con validación temporal; selección del modelo candidato | scikit-learn, joblib |
| `…ProyectoFinal10` | 10 | Optimización de hiperparámetros con GridSearchCV y comparación antes/después | scikit-learn, joblib |

> Los notebooks fueron desarrollados en **Google Colab** y usan `/content/` como directorio base. Ver [sección 9](#9-requisitos-y-reproducción).

---

## 5. Modelado predictivo

### 5.1 Guía 8: predicción de incidentes diarios

Modelo sobre la serie diaria agregada, con variables disponibles antes del cierre del día (día de la semana, mes, despliegues, rezagos y media móvil).

| Modelo | MAE | RMSE | R² |
|---|---|---|---|
| Baseline (promedio) | 4.85 | 6.65 | -0.004 |
| Regresión lineal | **4.15** | **5.49** | **0.316** |

### 5.2 Guías 9 y 10: predicción de volumen diario de tickets

- **Variable objetivo:** `ticket_count` (agregado diario)
- **Predictores:** `deployment_count`, `incident_count`, `avg_response_time_ms`, `avg_error_rate_pct`, `dia_semana`, `mes`, `es_fin_semana`, `lag_1`, `lag_7`, `media_movil_7`
- **Partición:** split cronológico 80/20 (139 días de entrenamiento, 35 de prueba), sin mezcla aleatoria para evitar fuga temporal
- **Validación:** `TimeSeriesSplit`
- **Semilla:** `random_state = 42`

**Comparación de modelos (Guía 9):**

| Modelo | MAE test | RMSE test | R² test | Supera baseline |
|---|---|---|---|---|
| **Random Forest** | **14.87** | **17.63** | 0.049 | Sí |
| Regresión lineal | 15.27 | 18.39 | -0.034 | Sí |
| Baseline (media) | 15.39 | 18.28 | -0.022 | No |
| Árbol de decisión | 22.03 | 26.21 | -1.100 | No |

**Optimización (Guía 10):** GridSearchCV con 36 combinaciones × 4 folds temporales, en un `Pipeline` con `SimpleImputer` (mediana) y `RandomForestRegressor`.

| Hiperparámetro | Valores explorados | Mejor |
|---|---|---|
| `n_estimators` | 100, 200 | 200 |
| `max_depth` | 5, 10, None | 10 |
| `min_samples_leaf` | 1, 3, 5 | 5 |
| `max_features` | 0.7, 1.0 | 0.7 |

| Modelo | MAE test | MAE validación |
|---|---|---|
| Baseline | 15.385 | 12.395 |
| Guía 9 | 14.869 | 13.514 |
| Guía 10 (optimizado) | 14.863 | 13.300 |

La mejora de la optimización frente a la Guía 9 es marginal (**0.04 %**). El R² cercano a cero indica que, con los datos y la ventana actuales, el volumen diario de tickets es poco predecible a partir de estas variables. Esto se documenta como resultado, no como defecto del pipeline.

### 5.3 Artefactos

| Archivo | Descripción |
|---|---|
| `Modelos/modelo_candidato_guia09 .joblib` | Random Forest candidato de la Guía 9 |
| `Modelos/modelo_optimizado_guia10.joblib` | Pipeline optimizado de la Guía 10 |
| `documentos/metadata_guia10.csv` | Versiones (Python 3.13.18, scikit-learn 1.6.1), parámetros, tiempos y semilla |

Carga de un modelo:

```python
import joblib

modelo = joblib.load("content/Modelos/modelo_optimizado_guia10.joblib")
y_pred = modelo.predict(X_nuevo)  # X_nuevo con las 10 features listadas arriba
```

> Para evitar errores de compatibilidad, cargar los `.joblib` con la misma versión de scikit-learn con la que se entrenaron (1.6.1).

---

## 6. Gobernanza y privacidad

La política completa está en `documentos/politica_gobernanza_DevOps_Metrics_Analytics_ProyectoFinal04.md`.

**Clasificación de datos**

| Nivel | Ejemplo |
|---|---|
| Público | `avg_availability_pct` |
| Interno | Uso dentro del equipo |
| Confidencial | `company_id`, `total_downtime_min` |
| Restringido | Parquet original |

**Controles técnicos**

- **Seudonimización:** `company_id` se reemplaza por `company_token`, los primeros 12 caracteres de `SHA-256(SALT | company_id)`.
- **Minimización:** lista blanca de columnas en la vista analítica; se excluye `total_downtime_min`.
- **Control de acceso por rol:**

| Rol | Acceso |
|---|---|
| Líder | Total; define la política de datos |
| Ingeniero de Datos | Datos crudos, para aplicar limpieza y minimización |
| Analista de Datos / Analista de Calidad | Solo vista protegida, sin identificadores |
| Docente | Solo lectura de evidencias |

- **Retención:** los datos se conservan durante el ciclo IV 2026 y se eliminan el 20/11/2026.
- **Riesgos:** matriz de riesgos en `documentos/Matriz_Riesgos_DevOps_Metrics_Analytics_ProyectoFinal04.xlsx`; clasificación de columnas en `documentos/clasificacion_datos_*.xlsx`.

---

## 7. Reportería automatizada

El pipeline de reportería genera tres entregables a partir de los datos procesados:

| Salida | Archivo |
|---|---|
| PDF | `exportaciones/Reporte_Gerencial_DevOps_v1.0.pdf` |
| Word | `exportaciones/Reporte_Gerencial_DevOps_v1.0.docx` |
| HTML | `exportaciones/Reporte_DevOps_Metrics.html` |
| Metadatos | `exportaciones/metadatos.json`, `metadata_informe.json`, `indicadores.csv`, `equipo.json`, `integrantes.csv` |

**Flujo de CI (`generar_reporte_devops.yml`):**

- Disparadores: cron semanal (lunes 08:00 UTC) y ejecución manual (`workflow_dispatch`)
- Entorno: `ubuntu-latest`, Python 3.10
- Dependencias: `pandas matplotlib jinja2 weasyprint python-docx pyarrow openpyxl` (más `libharfbuzz-subset0` del sistema)
- Ejecuta `scripts/pipeline_reporteria_devops.py` y publica `exportaciones/` como artefacto

Los gráficos del reporte están en `evidencias/reporte/` (downtime, calidad, gobernanza, prioridad, incidentes, departamentos).

---

## 8. KPIs del periodo

| Indicador | Valor |
|---|---|
| Total de despliegues | 15,000 |
| Tasa de fallo de despliegues | 10.0 % |
| Total de rollbacks | 1,500 |
| Total de incidentes | 4,000 |
| Lead time promedio | 12.53 h |
| Tiempo de resolución promedio | 6.31 h |
| Disponibilidad promedio | 97.48 % |
| Downtime total | 20,176.5 h |
| Empresas anonimizadas | 25 |

---

## 9. Requisitos y reproducción

### 9.1 Requisitos

- Python 3.10 o superior
- Java 8/11/17 (requerido por PySpark)

```bash
pip install pandas numpy matplotlib scikit-learn==1.6.1 joblib pyspark \
            jinja2 weasyprint python-docx pyarrow openpyxl tabulate
```

### 9.2 Ejecución en Google Colab (entorno original)

1. Subir la carpeta `content/` al entorno de Colab (los notebooks asumen `/content/` como raíz).
2. Ejecutar los notebooks en orden, del `01` al `10`: cada uno consume la salida del anterior.

### 9.3 Ejecución local

1. Clonar el repositorio:
   ```bash
   git clone https://github.com/sebaas256/Proyecto_final_gestion_de_datos_IA.git
   cd Proyecto_final_gestion_de_datos_IA
   ```
2. En cada notebook, ajustar la ruta base:
   ```python
   BASE_DIR = Path("content")   # en lugar de Path("/content/")
   ```
3. Omitir o comentar las celdas que usan `google.colab` (`files.download`, `drive.mount`).

### 9.4 Dashboard

Abrir `visualizaciones/devopss.pbix` con Power BI Desktop. Fuente recomendada: `datos/procesados/dataset_power_bi.csv` o `datos/geo/dataset_devops_geografico.csv` para los mapas.

---

## 10. Limitaciones conocidas

- **Datos sintéticos:** la dimensión geográfica es sintética (`tipo_ubicacion = Sintetica`); los dashboards geográficos son ilustrativos.
- **Muestra pequeña para ML:** 174 días de serie agregada (139 train / 35 test) limitan la capacidad predictiva. El R² del modelo de tickets es bajo.
- **Rutas dependientes de Colab:** los notebooks usan `/content/` y utilidades de `google.colab`.
- **Sin `requirements.txt`:** las dependencias se listan en este README, pero no existe un archivo de versiones fijado.
- **Imputación con ceros:** en la preparación para Power BI, los nulos de telemetría se rellenan con `0`, lo que puede sesgar promedios. La bandera `sin_datos_monitoreo` permite filtrarlos.
- **Nombres de archivo con espacios:** algunos archivos (`modelo_candidato_guia09 .joblib`, `predicciones_guia09 .csv`) incluyen un espacio antes de la extensión.

---

## 11. Equipo

**DevOps Analytics Team**

| Integrante | Rol |
|---|---|
| Sebastián González | Líder |
| Alan Aguirre | Ingeniero de Datos |
| Kevin Hernández | Analista de Datos |
| Alexis Pérez | Analista de Calidad |

---

## Licencia

Proyecto académico. Definir licencia antes de redistribuir.
