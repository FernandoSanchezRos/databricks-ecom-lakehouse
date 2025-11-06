## 1. Preparación del Entorno

Antes de comenzar a ingerir y transformar datos, se realiza una configuración inicial del **entorno Lakehouse** en Azure Databricks. El objetivo es definir dónde se almacenan los datos, cómo se gobiernan y qué componentes maneja Unity Catalog.

### 1.1 Estructura en Azure

```
lakehouse/
│
├── files/                        
│   ├── landing/                  
│   └── checkpoints/              
│
├── bronze/                       
├── silver/                       
└── gold/                         
```

| Carpeta | Contenido | Propósito |
|--------|-----------|-----------|
| `files/landing/` | Datos crudos (CSV/JSON/Parquet) | Zona de **ingesta** |
| `files/checkpoints/` | Logs internos de streaming | Permite cargas **incrementales** |
| `bronze/` | Tablas Delta recién ingeridas | Registro trazable del dato bruto |
| `silver/` | Tablas Delta limpias | Datos estandarizados y consistentes |
| `gold/` | Tablas para BI | Consumo desde Power BI / dashboards |

---

### 1.2 Estructura en Unity Catalog

```
ecom_lakehouse (catalog)
│
├── files (schema)
│   └── landing (external volume)
│
├── bronze (schema managed)
├── silver (schema managed)
└── gold (schema external)
```

| Recurso | Tipo | Propósito |
|--------|------|-----------|
| `ecom_lakehouse.files.landing` | Volume externo | Leer archivos crudos desde notebooks |
| `ecom_lakehouse.bronze` | Schema Managed | Tablas Delta ingestadas |
| `ecom_lakehouse.silver` | Schema Managed | Tablas refinadas |
| `ecom_lakehouse.gold` | Schema External | Tablas finales para BI |

---

### 1.3 External Locations

| External Location | Ruta | Propósito |
|------------------|------|-----------|
| `landing_ext_loc` | `/files/landing/` | Permite leer nuevos archivos en ingesta |
| `gold_ext_loc` | `/gold/` | Permite exponer datos para consumo externo |

---

### 1.4 Notebook `00_environment_setup`

Este notebook:
- Crea el catálogo `ecom_lakehouse`
- Configura la **storage credential**
- Declara las **external locations**
- Crea el **volume landing**
- Crea los schemas `bronze`, `silver`, `gold`

**No crea tablas** — estas se generan en notebooks posteriores de ingesta y transformación.

