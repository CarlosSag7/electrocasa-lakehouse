from pyspark.sql import functions as F
import pyspark.pipelines as dp


def preparar_ventas():

    df = spark.readStream.table(
        "electrocasa_dev.bronze.ventas_bronze"
    )

    # Limpiar espacios y convertir vacíos en NULL
    columnas_texto = [
        "venta_id", "sucursal_id", "producto_id",
        "vendedor_id", "cantidad", "monto_total",
        "metodo_pago", "fecha_venta", "canal"
    ]

    for columna in columnas_texto:
        df = df.withColumn(
            columna,
            F.when(
                F.trim(F.col(columna)) == "",
                None
            ).otherwise(F.trim(F.col(columna)))
        )

    # Cantidad
    df = df.withColumn(
        "cantidad",
        F.expr("try_cast(cantidad AS INT)")
    )

    # Normalizar importes europeos
    
    monto_texto = F.col("monto_total").cast("string")

    patron_europeo_miles = r"^-?\d{1,3}(\.\d{3})+,\d{2}$"
    patron_europeo_simple = r"^-?\d+,\d{2}$"

    df = df.withColumn(
        "monto_normalizado",
        F.when(
            monto_texto.rlike(patron_europeo_miles),
            F.regexp_replace(
                F.regexp_replace(monto_texto, r"\.", ""),
                ",", "."
            )
        ).when(
            monto_texto.rlike(patron_europeo_simple),
            F.regexp_replace(monto_texto, ",", ".")
        ).otherwise(monto_texto)
    )

    df = df.withColumn(
        "monto_total",
        F.expr("try_cast(monto_normalizado AS DECIMAL(12,2))")
    ).drop("monto_normalizado")

    # Normalizar fechas
    fecha = F.col("fecha_venta")

    df = df.withColumn(
        "fecha_venta",
        F.coalesce(
            F.to_date(fecha, "yyyy-MM-dd"),
            F.to_date(fecha, "dd/MM/yyyy"),
            F.to_date(
                F.to_timestamp(
                    fecha, "yyyy-MM-dd'T'HH:mm:ss"
                )
            )
        )
    )

    # Normalizar métodos de pago
    pago = F.upper(F.col("metodo_pago"))

    df = df.withColumn(
        "metodo_pago",
        F.when(pago.isin("EFECTIVO", "EFV"), "EFECTIVO")
        .when(pago == "PLIN", "PLIN")
        .when(pago == "YAPE", "YAPE")
        .when(pago.isin("TRF", "TRANSFERENCIA"), "TRANSFERENCIA")
        .when(
            pago.isin("TC", "TARJETA DE CREDITO"),
            "TARJETA_CREDITO"
        )
        .when(pago == "TD", "TARJETA_DEBITO")
        .when(pago == "TARJETA", "TARJETA")
    )

    # Normalizar canal
    df = df.withColumn(
        "canal",
        F.lower(F.col("canal"))
    )

    return df


# Identificar registros válidos y rechazados
def clasificar_ventas():

    df = preparar_ventas()

    motivo = F.concat_ws(
        "; ",
        F.when(
            F.col("venta_id").isNull(),
            "venta_id vacío"
        ),
        F.when(
            F.col("producto_id").isNull(),
            "producto_id vacío"
        ),
        F.when(
            F.col("cantidad").isNull() | (F.col("cantidad") <= 0),
            "cantidad inválida"
        ),
        F.when(
            F.col("monto_total").isNull() | (F.col("monto_total") <= 0),
            "monto_total inválido"
        ),
        F.when(
            F.col("fecha_venta").isNull(),
            "fecha_venta inválida"
        ),
        F.when(
            F.col("metodo_pago").isNull(),
            "método de pago no reconocido"
        ),
        F.when(
            ~F.col("canal").isin("ecommerce", "tienda_fisica") |
            F.col("canal").isNull(),
            "canal inválido"
        ),
        F.when(
            (F.col("canal") == "tienda_fisica") &
            F.col("vendedor_id").isNull(),
            "vendedor_id obligatorio para tienda física"
        ),
        F.when(
            (F.col("canal") == "tienda_fisica") &
            F.col("sucursal_id").isNull(),
            "sucursal_id obligatorio para tienda física"
        )
    )

    return df.withColumn("motivo_rechazo", motivo)


# Tabla Silver: ventas válidas
@dp.table(
    name="electrocasa_dev.silver.ventas_silver",
    comment="Ventas normalizadas, validadas y deduplicadas"
)
def ventas_silver():

    df = clasificar_ventas()

    return (
        df.filter(F.col("motivo_rechazo") == "")
        .drop("motivo_rechazo")
        .dropDuplicates(["venta_id"])
    )


# Tabla Silver: ventas rechazadas
@dp.table(
    name="electrocasa_dev.silver.ventas_rechazadas",
    comment="Ventas rechazadas con sus respectivos motivos"
)
def ventas_rechazadas():

    df = clasificar_ventas()

    return df.filter(
        F.col("motivo_rechazo") != ""
    )