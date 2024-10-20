from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, Integer, ForeignKey
import csv

# Stwórz silnik i metadane
engine = create_engine('sqlite:///weatherdata.db')
metadata = MetaData()

# Zdefiniuj tabele
stations_table = Table('stations', metadata,
    Column('station', String, primary_key=True),
    Column('latitude', Float),
    Column('longitude', Float),
    Column('elevation', Float),
    Column('name', String),
    Column('country', String),
    Column('state', String)
)

measure_table = Table('measure', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('station', String, ForeignKey('stations.station')),
    
    Column('date', String),
    Column('precip', Float),
    Column('tobs', Float)
)

# Stwórz tabele w bazie danych
metadata.create_all(engine)

# Wczytaj dane ze stacji
with open('clean_stations.csv', mode='r') as file:
    reader = csv.DictReader(file)
    stations_data = [row for row in reader]

# Wczytaj dane z pomiarów
with open('clean_measure.csv', mode='r') as file:
    reader = csv.DictReader(file)
    measures_data = [row for row in reader]

# Dodaj unikalne wartości 'id' ręcznie do każdego rekordu
ids = set()
for index, measure in enumerate(measures_data):
    while index + 1 in ids:
        index += 1
    ids.add(index + 1)
    measure['id'] = index + 1

# Wstaw dane do tabeli 'stations'
with engine.connect() as conn:
    conn.execute(stations_table.insert(), stations_data)

# # Wstaw dane do tabeli 'measure'
with engine.connect() as conn:
    conn.execute(measure_table.insert(), measures_data)

# Wykonaj zapytanie SQL, aby sprawdzić dane
with engine.connect() as conn:
    result_stations = conn.execute("SELECT * FROM stations LIMIT 5").fetchall()
    for row in result_stations:
        print(row)
    
    result_maxprecip = conn.execute("SELECT max(precip) FROM measure").fetchone()
    print(f"Max precip: {result_maxprecip}")


    result_top5_tobs = conn.execute("SELECT * FROM Measure order by tobs desc Limit 10").fetchall()
    print("TOP10 Temperature observed")
    for row in result_top5_tobs:
        print(row)
