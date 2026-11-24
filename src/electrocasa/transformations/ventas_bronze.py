
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)

import pyspark.pipelines as dp

# --------------------------------------------------
# 1. Ruta de origen
# --------------------------------------------------

RUTA_VENTAS = (
    "/Volumes/electrocasa_dev/default/"
    "vol_landing/ventas/"
)

# --------------------------------------------------
# 2. Esquema explícito de Ventas
# --------------------------------------------------

ESQUEMA_VENTAS = StructType([
    StructField("venta_id", StringType(), True),
    StructField("sucursal_id", StringType(), True),
    StructField("producto_id", StringType(), True),
    StructField("vendedor_id", StringType(), True),
    StructField("cantidad", StringType(), True),
    StructField("monto_total", StringType(), True),
    StructField("metodo_pago", StringType(), True),
    StructField("fecha_venta", StringType(), True),
    StructField("canal", StringType(), True)
])

# --------------------------------------------------
# 3. Tabla Bronze mediante Auto Loader
# --------------------------------------------------

@dp.table(
    name="ventas_bronze",
    comment="Datos originales de ventas ingeridos mediante Auto Loader"
)
def ventas_bronze():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("delimiter", ",")
        .schema(ESQUEMA_VENTAS)
        .load(RUTA_VENTAS)
    )
