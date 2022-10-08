import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt


def main():
    user, password, host, port, dbname = "postgres", "Simon", "localhost", "5432", "postgres"
    with create_engine(f'postgresql+psycopg2://{user}:{password}@{host}/{dbname}').connect() as conn:
        sql = "SELECT geschwindigkeit, ort FROM mart.f_fzg_messung JOIN mart.d_ort USING(d_ort_id);"
        pd.read_sql_query(sql, conn).boxplot(by="ort")
        plt.suptitle("")
        plt.ylabel("Geschwindigkeit in km/h")
        plt.xlabel("Ort")
        plt.title("Geschwindigkeit nach Ort")
        plt.savefig("Boxplot.png")


if __name__ == '__main__':
    main()
