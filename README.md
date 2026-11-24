
# ElectroCasa Lakehouse

Proyecto de implementación de una arquitectura Lakehouse sobre Azure Databricks, aplicando el patrón Medallion (Bronze, Silver y Gold), contratos de datos, controles de calidad e infraestructura como código.

## 1. Objetivo del proyecto

Construir una plataforma de datos que permita integrar, procesar, validar y transformar información de diferentes fuentes de datos, preparando los datos para su posterior explotación.

La arquitectura se desarrolla sobre Databricks y Unity Catalog, utilizando tablas Delta y procesos de ingesta y transformación.

## 2. Herramientas utilizadas

- Azure Databricks.
- Databricks Asset Bundles.
- Unity Catalog.
- Delta Lake.
- Apache Spark y PySpark.
- Python.
- SQL.
- Auto Loader.
- COPY INTO.
- Git y GitHub.

## 3. Arquitectura

El proyecto sigue el patrón Medallion:

| Capa | Responsabilidad |
|---|---|
| Bronze | Ingesta de datos originales y conservación de la información recibida. |
| Silver | Normalización, depuración, deduplicación y aplicación de reglas de calidad. |
| Gold | Preparación de información consolidada para consultas e indicadores de negocio. |

Los datos se organizan en Unity Catalog mediante catálogos y esquemas.

Entorno de desarrollo:

- Catálogo: 'electrocasa_dev'
- Esquemas: 'bronze', 'silver' y 'gold'
- Volumen de entrada: 'default.vol_landing'

## 4. Fuentes de datos y avance

El desarrollo se está realizando progresivamente por fuente.

| Fuente | Ingesta | Bronze | Silver | Estado |
|---|---|---|---|---|
| Ventas | Auto Loader | Implementado | Implementado | Desarrollado |
| Catálogo de productos | COPY INTO | Implementado | Implementado | Desarrollado |
| Empleados | Pendiente | Pendiente | Pendiente | Pendiente |
| Reseñas | Pendiente | Pendiente | Pendiente | Pendiente |
| Devoluciones | Pendiente | Pendiente | Pendiente | Pendiente |
| Sucursales | Pendiente | Pendiente | Pendiente | Pendiente |
| Tracking de envíos | Pendiente | Pendiente | Pendiente | Pendiente |

## 5. Procesos implementados

### 5.1 Ventas

Ingesta mediante Auto Loader desde el volumen de landing.

Transformaciones implementadas:
- Definición explícita del esquema de entrada.
- Procesamiento de archivos CSV.
- Normalización de datos.
- Identificación y tratamiento de registros duplicados.
- Validación de reglas de calidad.
- Separación de registros rechazados.

Resultados obtenidos durante las pruebas:

| Indicador | Registros |
|---|---:|
| Bronze | 13.622 |
| Silver | 13.132 |
| Rechazados | 372 |
| Duplicados exactos excedentes | 118 |

Se verificó la reconciliación de registros:

'13.622 = 13.132 + 372 + 118'

### 5.2 Catálogo de productos

Ingesta mediante COPY INTO desde archivos JSON correspondientes a dos snapshots del catálogo.

Archivos procesados:
- `catalogo_productos_2026-03-01.json`
- `catalogo_productos_2026-08-01.json`

Transformaciones implementadas:
- Lectura de JSON multiline.
- Definición explícita del esquema.
- Normalización de categorías.
- Conversión y validación de precios.
- Identificación de conflictos y registros inválidos.
- Conservación de información histórica de los snapshots.
- Separación de registros rechazados.

Resultados obtenidos:

| Indicador | Registros |
|---|---:|
| Bronze | 4.705 |
| Silver | 4.563 |
| Rechazados | 141 |

Se realizaron pruebas de reejecución de COPY INTO, verificando que los archivos ya procesados no generaran nuevas inserciones.

## 6. Contratos de datos

Se han desarrollado contratos de datos en formato YAML para documentar las características y reglas de las fuentes procesadas.

Contratos implementados:
- `contratos/ventas.yml`
- `contratos/catalogo.yml`

Los contratos documentan aspectos como:
- Identificación de la fuente.
- Estructura y tipos de datos.
- Reglas de calidad.
- Transformaciones aplicadas.
- Problemas conocidos de los datos.

## 7. Databricks Asset Bundles y CI/CD

Se ha configurado Databricks Asset Bundles para administrar los recursos del proyecto.

Archivo principal:
- `databricks.yml`

Recursos:
- `resources/electrocasa_pipeline.yml`
- `resources/electrocasa_job.yml`

Se han definido dos targets:

| Target | Catálogo |
|---|---|
| dev | `electrocasa_dev` |
| prod | `electrocasa_prod` |

Ambos targets utilizan el mismo workspace de Databricks, con rutas independientes de despliegue.

Se han ejecutado satisfactoriamente las siguientes validaciones:

```powershell
databricks bundle validate -t dev
databricks bundle validate -t prod
```

**Nota:** la configuración de ambos targets está validada. El despliegue y la parametrización completa de los recursos para producción todavía están pendientes.

## 8. Estructura del repositorio

```text
electrocasa-lakehouse/
│
├── contratos/
│   ├── ventas.yml
│   └── catalogo.yml
│
├── notebooks/
│   └── 00_setup.ipynb
│
├── resources/
│   ├── electrocasa_pipeline.yml
│   └── electrocasa_job.yml
│
├── src/
│   └── electrocasa/
│       ├── ingestion/
│       │   └── catalogo_copy_into.sql
│       └── transformations/
│           ├── ventas_bronze.py
│           ├── ventas_silver.py
│           └── catalogo_silver.py
│
├── databricks.yml
└── README.md
```

## 9. Estado actual y próximos desarrollos

El proyecto se encuentra en desarrollo incremental.

Componentes implementados:
- Ingesta Bronze de Ventas mediante Auto Loader.
- Transformación Silver de Ventas.
- Ingesta Bronze de Catálogo mediante COPY INTO.
- Transformación Silver de Catálogo.
- Contratos de datos para ambas fuentes.
- Pipeline administrado mediante Databricks Asset Bundles.
- Configuración y validación de los targets dev y prod.

Componentes pendientes:
- Incorporación de las fuentes restantes.
- Desarrollo de las tablas Gold y consultas de negocio.
- Implementación del proceso de Tracking mediante Lakeflow Connect.
- Configuración del Job de orquestación.
- Implementación de grupos y permisos de Unity Catalog.
- Políticas de enmascaramiento de datos sensibles.
- Monitorización y evidencias de auditoría.
- Despliegue completo del entorno de producción.

## 10. Repositorio

Código fuente del proyecto:

https://github.com/CarlosSag7/electrocasa-lakehouse
