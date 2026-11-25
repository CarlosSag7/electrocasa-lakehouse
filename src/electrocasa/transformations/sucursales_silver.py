from pyspark.sql import functions as F
import pyspark.pipelines as dp


def preparar_sucursales():

    df = spark.readStream.table(
        "electrocasa_dev.bronze.sucursales_bronze"
    )

    # Limpiar espacios y convertir cadenas vacias en NULL
    columnas_texto = [
        "sucursal_id",
        "nombre_sucursal",
        "distrito",
        "ciudad",
        "region",
        "fecha_apertura"
    ]

    for columna in columnas_texto:
        df = df.withColumn(
            columna,
            F.when(
                F.trim(F.col(columna)) == "",
                None
            ).otherwise(F.trim(F.col(columna)))
        )

    # Normalizar identificador
    df = df.withColumn(
        "sucursal_id",
        F.upper(F.col("sucursal_id"))
    )

    # Normalizar nombres geograficos
    df = (
        df
        .withColumn("distrito", F.initcap(F.col("distrito")))
        .withColumn("ciudad", F.initcap(F.col("ciudad")))
        .withColumn("region", F.initcap(F.col("region")))
    )

    # Convertir fecha de apertura a DATE
    df = df.withColumn(
        "fecha_apertura",
        F.expr("try_cast(fecha_apertura AS DATE)")
    )

    return df


def clasificar_sucursales():

    df = preparar_sucursales()

    motivo = F.concat_ws(
        "; ",
        F.when(
            F.col("sucursal_id").isNull(),
            "sucursal_id obligatorio"
        ),
        F.when(
            F.col("nombre_sucursal").isNull(),
            "nombre_sucursal obligatorio"
        ),
        F.when(
            F.col("distrito").isNull(),
            "distrito obligatorio"
        ),
        F.when(
            F.col("ciudad").isNull(),
            "ciudad obligatoria"
        ),
        F.when(
            F.col("region").isNull(),
            "region obligatoria"
        ),
        F.when(
            F.col("fecha_apertura").isNull(),
            "fecha_apertura invalida"
        )
    )

    return df.withColumn("motivo_rechazo", motivo)


# Tabla Silver: sucursales validas
@dp.table(
    name="electrocasa_dev.silver.sucursales_silver",
    comment="Sucursales normalizadas y validadas"
)
def sucursales_silver():

    df = clasificar_sucursales()

    return (
        df.filter(F.col("motivo_rechazo") == "")
        .drop("motivo_rechazo")
        .dropDuplicates(["sucursal_id"])
    )


# Tabla Silver: sucursales rechazadas
@dp.table(
    name="electrocasa_dev.silver.sucursales_rechazadas",
    comment="Sucursales rechazadas con sus respectivos motivos"
)
def sucursales_rechazadas():

    df = clasificar_sucursales()

    return df.filter(
        F.col("motivo_rechazo") != ""
    )
