from sqlalchemy import create_engine

engine = create_engine("postgresql://postgres:secretsd@localhost:5432/retail")
conn = engine.connect()
print("Connected successfully!")
conn.close()
