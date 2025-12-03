import random
import csv
import os
from faker import Faker
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import make_url
from dotenv import load_dotenv

N_ROWS = 100_000      # cámbialo fácil aquí
CHUNK  = 5_000        # tamaño de bloque para CSV y BD


def ensure_database(db_url: str) -> None:
    """Create the target MySQL database if it doesn't exist."""
    url = make_url(db_url)
    dbname = url.database
    if not dbname:
        return
    server_url = url.set(database=None)
    engine = create_engine(server_url, future=True)
    with engine.begin() as conn:
        print(f"Creating database {dbname}")
        conn.execute(
            text(
                f"CREATE DATABASE IF NOT EXISTS `{dbname}` "
                "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            )
        )


def generar_filas(fake: Faker, n: int, null_prob: float):
    """Generador de diccionarios con datos falsos."""
    for _ in range(n):
        yield {
            "nombre": fake.name(),
            "email": fake.email(),
            "direccion": fake.address().replace("\n", ", "),
            "telefono": fake.phone_number() if random.random() > null_prob else None,
            "fecha_nacimiento": fake.date_of_birth(minimum_age=18, maximum_age=80),
            "cedula": fake.ssn(),
            "fecha_registro": fake.date_between(start_date='-5y', end_date='today'),
            "fecha_pago": (
                fake.date_between(start_date='-1y', end_date='today')
                if random.random() > null_prob else None
            ),
        }


def main() -> None:
    fake = Faker('es_ES')
    null_prob = 0.1

    fieldnames = [
        "nombre", "email", "direccion", "telefono",
        "fecha_nacimiento", "cedula", "fecha_registro", "fecha_pago"
    ]

    # 1) Generar y escribir CSV por bloques
    gen = generar_filas(fake, N_ROWS, null_prob)

    with open("datos_falsos.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        while True:
            bloque = [next(gen, None) for _ in range(CHUNK)]
            bloque = [r for r in bloque if r is not None]
            if not bloque:
                break
            writer.writerows(bloque)

    # 2) Preparar conexión a BD
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("DATABASE_URL no está definida. Se omite la ingesta.")
        return

    ensure_database(db_url)
    engine = create_engine(db_url, future=True)

    # 3) Crear tabla e insertar CSV en bloques
    with engine.begin() as conn:
        conn.execute(text(
            """
            CREATE TABLE IF NOT EXISTS datos_falsos (
                id INT AUTO_INCREMENT PRIMARY KEY,
                nombre VARCHAR(255) NOT NULL,
                email VARCHAR(255) NOT NULL,
                direccion TEXT NOT NULL,
                telefono VARCHAR(50) DEFAULT NULL,
                fecha_nacimiento DATE NOT NULL,
                cedula VARCHAR(50) NOT NULL,
                fecha_registro DATE NOT NULL,
                fecha_pago DATE DEFAULT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
        ))

        with open("datos_falsos.csv", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            bloque = []
            for row in reader:
                bloque.append({
                    "nombre": row["nombre"],
                    "email": row["email"],
                    "direccion": row["direccion"],
                    "telefono": row["telefono"],          # puede ser '' → MySQL lo interpreta según config
                    "fecha_nacimiento": row["fecha_nacimiento"],
                    "cedula": row["cedula"],
                    "fecha_registro": row["fecha_registro"],
                    "fecha_pago": row["fecha_pago"] or None,
                })
                if len(bloque) == CHUNK:
                    conn.execute(
                        text(
                            """
                            INSERT INTO datos_falsos
                            (nombre, email, direccion, telefono,
                             fecha_nacimiento, cedula, fecha_registro, fecha_pago)
                            VALUES
                            (:nombre, :email, :direccion, :telefono,
                             :fecha_nacimiento, :cedula, :fecha_registro, :fecha_pago)
                            """
                        ),
                        bloque,
                    )
                    bloque = []

            if bloque:
                conn.execute(
                    text(
                        """
                        INSERT INTO datos_falsos
                        (nombre, email, direccion, telefono,
                         fecha_nacimiento, cedula, fecha_registro, fecha_pago)
                        VALUES
                        (:nombre, :email, :direccion, :telefono,
                         :fecha_nacimiento, :cedula, :fecha_registro, :fecha_pago)
                        """
                    ),
                    bloque,
                )

    print("CSV generado e insertado en la base de datos.")


if __name__ == "__main__":
    main()
