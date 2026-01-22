import pandas as pd

def run_etl():
    """
    Implementa el proceso ETL.
    No cambies el nombre de esta función.
    """
    df = pd.read_csv("data/citas_clinica.csv")

    #Transformaciones
    df["paciente"] = df["paciente"].str.title()
    df["especialidad"] = df["especialidad"].str.upper()

    #Fechas
    df["fecha_cita"] = pd.to_datetime(df["fecha_cita"], format="%Y-%m-%d", errors="coerce")
    df = df.dropna(subset=["fecha_cita"])

    #Reglas de negocio
    df = df[df["estado"] == "CONFIRMADA"]
    df = df[df["costo"] > 0]

    #Valores nulos
    df["telefono"] = df["telefono"].fillna("NO REGISTRA")


    df.to_csv("data/output.csv", index=False)



if __name__ == "__main__":
    run_etl()
