import datetime

import psycopg2


def main():
    # Connect to database
    postgres = psycopg2.connect("dbname='postgres' user='postgres' password='Simon' host='localhost' port='5432'")
    postgres.autocommit = True
    cursor = postgres.cursor()

    # ---------- [Customer Dimension] ---------- #
    print("Inserting customers...")

    # Get the data
    cursor.execute("""
        SELECT kunde_id,
           vorname,
           nachname,
           anrede,
           geschlecht,
           geburtsdatum,
           wohnort,
           ort.ort,
           land.land
        FROM (staging.kunde LEFT OUTER JOIN staging.ort ort on ort.ort_id = kunde.wohnort)
                 LEFT OUTER JOIN staging.land land on land.land_id = ort.land_id;
    """)
    customer_data = cursor.fetchall()

    # Insert it into the mart
    for customer in customer_data:
        try:
            cursor.execute(f"""
                INSERT INTO mart.d_kunde 
                    (kunde_id, vorname, nachname, anrede, geschlecht, geburtsdatum, wohnort_id, ort, land)
                VALUES (
                    {int(customer[0])},
                    '{customer[1]}',
                    '{customer[2]}',
                    '{customer[3]}',
                    '{customer[4]}',
                    '{str(customer[5])}',
                    {int(customer[6])},
                    '{customer[7]}',
                    '{customer[8]}'
                );
            """)
        except Exception as e:
            print(f"Error while inserting the row {customer} into the database: {e}")
    print("Done!")

    # ---------- [Vehicle Dimension] ---------- #
    print("Inserting vehicles...")

    # Get the data
    cursor.execute("""
        SELECT 
            fahrzeug.fin,
            kfz.kfz_kennzeichen,
            baujahr,
            modell,
            hersteller.hersteller
        FROM (staging.fahrzeug
            LEFT OUTER JOIN staging.hersteller hersteller on fahrzeug.hersteller_code = hersteller.hersteller_code)
                 LEFT OUTER JOIN staging.kfzzuordnung kfz on fahrzeug.fin = kfz.fin;
    """)
    vehicle_data = cursor.fetchall()

    # Insert it into the mart
    for vehicle in vehicle_data:
        try:
            cursor.execute(f"""
                INSERT INTO mart.d_fahrzeug (fin, kfz_kennzeichen, baujahr, modell, hersteller)
                VALUES {vehicle};
            """)
        except Exception as e:
            print(f"Error while inserting the row {vehicle} into the database: {e}")
    print("Done!")

    # ---------- [Location Dimension] ---------- #
    print("Inserting locations...")

    # Get the data
    cursor.execute("""
        SELECT ort.ort, land.land
        FROM staging.ort LEFT OUTER JOIN staging.land land on ort.land_id = land.land_id;
    """)
    measurement_data = cursor.fetchall()

    # Insert it into the mart
    for location in measurement_data:
        try:
            cursor.execute(f"""
                INSERT INTO mart.d_ort (ort, land) 
                VALUES (
                    '{location[0]}',
                    '{location[1] if location[1] is not None else ''}'
                );
            """)
        except Exception as e:
            print(f"Error while inserting the row {location} into the database: {e}")
    print("Done!")

    # ---------- [Measurement facts] ---------- #
    print("Inserting measurements...")

    # Get the data
    cursor.execute("""
        SELECT payload, erstellt_am
        FROM staging.messung;
    """)
    measurement_data = cursor.fetchall()

    # Insert it into the mart
    for measurement in measurement_data:
        try:
            payload = measurement[0]
            fin = payload["fin"]
            speed = payload["geschwindigkeit"]

            # Get the data mart ids
            cursor.execute(f"SELECT d_fahrzeug_id FROM mart.d_fahrzeug WHERE fin = '{fin}';")
            vehicle_id = cursor.fetchone()[0]

            cursor.execute(f"SELECT kunde_id FROM staging.fahrzeug WHERE fin = '{fin}';")
            customer_id = cursor.fetchone()[0]

            cursor.execute(f"SELECT d_kunde_id, ort FROM mart.d_kunde WHERE kunde_id = {customer_id};")
            customer_id, location = cursor.fetchone()

            cursor.execute(f"SELECT d_ort_id FROM mart.d_ort WHERE ort = '{location}';")
            location_id = cursor.fetchone()[0]

            query = f"""
                INSERT INTO mart.f_fzg_messung 
                SELECT 
                    '{measurement[1].strftime('%d.%m.%Y %H:%M:%S.%f')[:-3]}',
                    '{datetime.datetime.now().strftime('%d.%m.%Y %H:%M:%S.%f')[:-3]}',
                    {customer_id},
                    {vehicle_id},
                    {location_id},
                    {speed}
                ;
            """

            cursor.execute(query)
            postgres.commit()
        except Exception as e:
            print(f"Error while inserting the row {measurement} into the database: {e}")

    print("Done!")


if __name__ == '__main__':
    main()
