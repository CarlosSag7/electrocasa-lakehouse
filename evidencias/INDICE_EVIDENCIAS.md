
# Evidencias del proyecto ElectroCasa Lakehouse

Esta carpeta contiene las evidencias de implementación y validación de los componentes desarrollados en el proyecto.

## 1. Ingesta de datos

- [ ] Ingesta Bronze de Ventas mediante Auto Loader.
- [ ] Ingesta Bronze del Catálogo mediante COPY INTO.
- [ ] Validación de idempotencia de COPY INTO.

## 2. Transformación y calidad de datos

- [ ] Resultados de transformación Silver de Ventas.
- [ ] Resultados de transformación Silver del Catálogo.
- [ ] Conteos de registros aceptados y rechazados.
- [ ] Validaciones de deduplicación y calidad.

## 3. Contratos de datos

- [ ] Contrato de Ventas.
- [ ] Contrato del Catálogo.

## 4. CI/CD

- [ ] Validación del target dev.
- [ ] Validación del target prod.

## 5. Componentes pendientes

Las evidencias de los componentes todavía no implementados se incorporarán conforme avance el desarrollo.


## 6. Resultados de procesamiento

### Ventas

| Indicador | Registros |
|---|---:|
| Bronze | 13.622 |
| Silver | 13.132 |
| Rechazados | 372 |
| Duplicados exactos excedentes | 118 |

Reconciliación: 13.622 = 13.132 + 372 + 118.

### Catálogo de productos

| Indicador | Registros |
|---|---:|
| Bronze | 4.705 |
| Silver | 4.563 |
| Rechazados | 141 |

Los resultados corresponden a las pruebas ejecutadas en el catálogo `electrocasa_dev`.


## 7. Capturas de evidencias disponibles

| Archivo | Descripción |
|---|---|
| 01_ventas_bronze.png | Conteo de registros en Bronze de Ventas. |
| 02_ventas_silver.png | Conteo de registros en Silver de Ventas. |
| 03_catalogo_productos.png | Conteo de registros en Bronze del Catálogo. |
| 04_silver_catalogo.png | Conteo de registros en Silver del Catálogo. |
| 05_registros_rechazados.png | Conteo de registros rechazados del Catálogo. |
