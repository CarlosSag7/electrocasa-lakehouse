
from pyspark.sql import functions as F
import pyspark.pipelines as dp


# ============================================================
# ELECTROCASA - TRANSFORMACION SILVER DEL CATALOGO
# Fuente: electrocasa_dev.bronze.catalogo_bronze
# Destino: electrocasa_dev.silver.catalogo_silver
# Rechazos: electrocasa_dev.silver.catalogo_rechazadas
# Procesamiento: snapshots completos por lotes
# ============================================================


def preparar_catalogo():

    df = spark.read.table(
        "electrocasa_dev.bronze.catalogo_bronze"
    )

    # --------------------------------------------------------
    # 1. Limpiar espacios y cadenas vacias
    # --------------------------------------------------------

    columnas_texto = [
        "producto_id",
        "nombre_producto",
        "categoria",
        "marca",
        "precio_lista"
    ]

    for columna in columnas_texto:
        df = df.withColumn(
            columna,
            F.when(
                F.trim(F.col(columna)) == "",
                None
            ).otherwise(F.trim(F.col(columna)))
        )

    # --------------------------------------------------------
    # 2. Eliminar duplicados exactamente iguales
    # --------------------------------------------------------

    columnas_negocio = [
        "producto_id",
        "nombre_producto",
        "categoria",
        "marca",
        "precio_lista",
        "activo",
        "fecha_snapshot",
        "_rescued_data"
    ]

    df = df.dropDuplicates(columnas_negocio)

    # --------------------------------------------------------
    # 3. Normalizar categorias
    # --------------------------------------------------------

    categoria = F.upper(F.trim(F.col("categoria")))

    categoria = F.translate(
        categoria,
        "ÁÉÍÓÚÜ",
        "AEIOUU"
    )

    categoria = F.regexp_replace(
        categoria,
        r"[_\s]+",
        " "
    )

    df = df.withColumn(
        "categoria",
        F.when(
            categoria == "CLIMATIZACION",
            "Climatización"
        )
        .when(
            categoria == "COCINA",
            "Cocina"
        )
        .when(
            categoria == "ELECTRONICA",
            "Electrónica"
        )
        .when(
            categoria == "ENTRETENIMIENTO",
            "Entretenimiento"
        )
        .when(
            categoria == "LINEA BLANCA",
            "Línea blanca"
        )
    )

    # --------------------------------------------------------
    # 4. Normalizar precios
    # --------------------------------------------------------

    precio_texto = F.col("precio_lista")

    precio_normalizado = F.regexp_replace(
        precio_texto,
        r"(?i)S/\s*",
        ""
    )

    precio_normalizado = F.regexp_replace(
        precio_normalizado,
        r"[,\s]",
        ""
    )

    df = df.withColumn(
        "precio_lista_original",
        precio_texto
    )

    df = df.withColumn(
        "precio_lista_normalizado",
        precio_normalizado
    )

    df = df.withColumn(
        "precio_lista",
        F.expr(
            "try_cast(precio_lista_normalizado AS DECIMAL(12,2))"
        )
    )

    # --------------------------------------------------------
    # 5. Detectar precios contradictorios
    #    para el mismo producto y snapshot
    # --------------------------------------------------------

    conflictos = (
        df.groupBy(
            "producto_id",
            "fecha_snapshot"
        )
        .agg(
            F.countDistinct(
                F.struct(F.col("precio_lista_normalizado"))
            ).alias("precios_distintos")
        )
        .filter(F.col("precios_distintos") > 1)
        .select(
            "producto_id",
            "fecha_snapshot"
        )
        .withColumn(
            "es_conflicto",
            F.lit(True)
        )
    )

    df = (
        df.join(
            conflictos,
            ["producto_id", "fecha_snapshot"],
            "left"
        )
        .withColumn(
            "es_conflicto",
            F.coalesce(
                F.col("es_conflicto"),
                F.lit(False)
            )
        )
    )

    return df


# ============================================================
# 6. Clasificar registros validos y rechazados
# ============================================================

def clasificar_catalogo():

    df = preparar_catalogo()

    motivo = F.concat_ws(
        "; ",
        F.when(
            F.col("producto_id").isNull(),
            "producto_id vacio"
        ),
        F.when(
            F.col("nombre_producto").isNull(),
            "nombre_producto vacio"
        ),
        F.when(
            F.col("categoria").isNull(),
            "categoria nula o no reconocida"
        ),
        F.when(
            F.col("precio_lista").isNull(),
            "precio nulo o no convertible"
        ),
        F.when(
            F.col("precio_lista") <= 0,
            "precio cero o negativo"
        ),
        F.when(
            F.col("activo").isNull(),
            "estado activo nulo"
        ),
        F.when(
            F.col("fecha_snapshot").isNull(),
            "fecha_snapshot nula"
        ),
        F.when(
            F.col("es_conflicto"),
            "producto con precios contradictorios en el mismo snapshot"
        ),
        F.when(
            F.col("_rescued_data").isNotNull(),
            "datos no reconocidos por el esquema"
        )
    )

    return df.withColumn(
        "motivo_rechazo",
        motivo
    )


# ============================================================
# 7. Tabla Silver: catalogo valido
# ============================================================

@dp.materialized_view(
    name="electrocasa_dev.silver.catalogo_silver",
    comment="Catalogo normalizado, validado e historizado por snapshot"
)
def catalogo_silver():

    df = clasificar_catalogo()

    return (
        df.filter(F.col("motivo_rechazo") == "")
        .select(
            "producto_id",
            "nombre_producto",
            "categoria",
            "marca",
            "precio_lista",
            "activo",
            "fecha_snapshot",
            "archivo_origen",
            "fecha_ingesta"
        )
    )


# ============================================================
# 8. Tabla Silver: registros rechazados
# ============================================================

@dp.materialized_view(
    name="electrocasa_dev.silver.catalogo_rechazadas",
    comment="Registros del catalogo rechazados con sus motivos"
)
def catalogo_rechazadas():

    df = clasificar_catalogo()

    return (
        df.filter(F.col("motivo_rechazo") != "")
        .drop("es_conflicto")
    )
