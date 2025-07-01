from config.database import get_db_connection
from data.athletes import ATHLETE_DATA

class AthleteDataPipeline:
    """Simplified pipeline without model classes."""
    
    def __init__(self):
        self.conn = get_db_connection()
    
    def create_table(self):
        """Create the athlete_tests table if it doesn't exist."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS athlete_tests (
            athlete_id INTEGER PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            age INTEGER NOT NULL,
            test_date DATE NOT NULL,
            test_type VARCHAR(50) NOT NULL,
            test_result NUMERIC(10, 2) NOT NULL,
            coach_comments TEXT
        )
        """
        
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(create_table_sql)
                self.conn.commit()
                print("Table created successfully")
        except Exception as e:
            self.conn.rollback()
            print(f"Error creating table: {e}")
            raise
    
    def prepare_data(self):
        """Convert dictionary data to tuples in correct order for insertion."""
        prepared_data = []
        for athlete in ATHLETE_DATA:
            prepared_data.append((
                athlete['athlete_id'],
                athlete['name'],
                athlete['age'],
                athlete['test_date'],
                athlete['test_type'],
                athlete['test_result'],
                athlete['coach_comments']
            ))
        return prepared_data
    
    def insert_data(self, data):
        """Insert data directly into the athlete_tests table."""
        insert_sql = """
        INSERT INTO athlete_tests (
            athlete_id, name, age, test_date, test_type, test_result, coach_comments
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (athlete_id) DO NOTHING
        """
        
        try:
            with self.conn.cursor() as cursor:
                cursor.executemany(insert_sql, data)
                self.conn.commit()
                print(f"Successfully inserted {len(data)} records")
        except Exception as e:
            self.conn.rollback()
            print(f"Error inserting data: {e}")
            raise
    
    def run_pipeline(self):
        """Run the complete data pipeline."""
        try:
            self.create_table()
            prepared_data = self.prepare_data()
            self.insert_data(prepared_data)
        finally:
            if self.conn:
                self.conn.close()