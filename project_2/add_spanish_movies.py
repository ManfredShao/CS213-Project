import psycopg2
import requests
import time

# ======================== Configuration ========================
# TMDB API key for accessing The Movie Database API
TMDB_API_KEY = "164b934b951df62873cfdd4aaf122f41"

# Number of Spanish movies to add per year
ADD_PER_YEAR = 10

# Database connection configuration
DB_CONFIG = {
    'dbname': 'films',
    'user': 'manfred',
    'password': '112358',
    'host': 'localhost',
    'port': '5432'
}
# ==============================================================

def get_db_connection():
    """Establish and return a PostgreSQL database connection.
    
    Returns:
        psycopg2.connection: Database connection object.
    """
    return psycopg2.connect(**DB_CONFIG)

def fetch_json(url, params):
    """Fetch JSON data from a URL with retry logic.
    
    Args:
        url (str): The URL to fetch from.
        params (dict): Query parameters.
        
    Returns:
        dict: JSON response or None if request fails after retries.
    """
    params['api_key'] = TMDB_API_KEY
    for _ in range(3):
        try:
            r = requests.get(url, params=params, timeout=10)
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 429:
                time.sleep(2)  # Rate limit: wait before retry
        except:
            pass
        time.sleep(0.3)
    return None

def get_person_dates(person_tmdb_id):
    """Fetch birth year and death year for a person from TMDB.
    
    Args:
        person_tmdb_id (int): The TMDB ID of the person.
        
    Returns:
        tuple: (birth_year, death_year) where death_year is None if still alive.
    """
    data = fetch_json(f"https://api.themoviedb.org/3/person/{person_tmdb_id}", {})
    born = 0
    died = None
    if data:
        if data.get('birthday'):
            try: born = int(data['birthday'][:4])
            except: born = 0
        if data.get('deathday'):
            try: died = int(data['deathday'][:4])
            except: died = None
    return born, died

def get_max_id(cur, table, col):
    """Get the maximum ID value from a table.
    
    Args:
        cur (psycopg2.cursor): Database cursor.
        table (str): Table name.
        col (str): ID column name.
        
    Returns:
        int: Maximum ID value or 0 if table is empty.
    """
    cur.execute(f"SELECT MAX({col}) FROM {table}")
    res = cur.fetchone()[0]
    return res if res is not None else 0

def process_spanish_movies():
    """Fetch and add Spanish movies from TMDB to the database.
    
    Retrieves Spanish movies from 2018-2025 and inserts them along with their
    directors and cast members into the database.
    """
    conn = get_db_connection()
    cur = conn.cursor()

    # Initialize ID counters
    current_max_movie_id = get_max_id(cur, 'movies', 'movieid')
    current_max_people_id = get_max_id(cur, 'people', 'peopleid')
    
    print(f"=== Starting to add Spanish movies (using country code 'es') ===")
    print(f"Starting MovieID: {current_max_movie_id}")

    try:
        for year in range(2018, 2026):
            print(f"--- Processing year: {year} ---")
            
            # Search criteria: language=es, region=ES, sorted by popularity
            data = fetch_json("https://api.themoviedb.org/3/discover/movie", {
                'primary_release_year': year,
                'sort_by': 'popularity.desc',
                'with_original_language': 'es',
                'region': 'ES',
                'page': 1
            })

            if not data or 'results' not in data: continue
            
            count_added = 0
            for m in data['results']:
                if count_added >= ADD_PER_YEAR: break
                
                tmdb_id = m['id']
                title = m['title'][:100]  # Truncate title to prevent overflow
                
                # Check for duplicate movies (avoid re-insertion)
                cur.execute("SELECT 1 FROM movies WHERE title=%s AND year_released=%s", (title, year))
                if cur.fetchone():
                    # Duplicate found, skip this movie
                    continue 

                # Fetch movie details to get runtime
                details = fetch_json(f"https://api.themoviedb.org/3/movie/{tmdb_id}", {})
                runtime = details.get('runtime', 0) if details else 0
                if runtime is None:
                    runtime = 0

                # Insert movie into database
                current_max_movie_id += 1
                movie_db_id = current_max_movie_id
                
                try:
                    # Set country code to 'es' for Spanish movies
                    cur.execute("""
                        INSERT INTO movies (movieid, title, country, year_released, runtime)
                        VALUES (%s, %s, 'es', %s, %s)
                    """, (movie_db_id, title, year, runtime))
                except psycopg2.Error as e:
                    conn.rollback()
                    current_max_movie_id -= 1
                    print(f"  [Error] Failed to insert movie {title}: {e}")
                    continue

                # Process credits: fetch directors and cast members
                credits = fetch_json(f"https://api.themoviedb.org/3/movie/{tmdb_id}/credits", {})
                if credits:
                    candidates = []
                    # Add directors
                    for x in credits.get('crew', []):
                        if x['job'] == 'Director':
                            candidates.append((x, 'D'))
                    # Add cast members (up to 5)
                    for x in credits.get('cast', [])[:5]:
                        candidates.append((x, 'A'))

                    for p_data, role in candidates:
                        # Parse person's name
                        full = p_data['name'].strip()
                        if ' ' in full:
                            parts = full.rsplit(' ', 1)
                            fname, sname = parts[0][:30], parts[1][:30]
                        else:
                            fname, sname = "", full[:30]
                        
                        # Map TMDB gender to our format: 1=Female, 2=Male, 0/other=Unknown
                        g_val = p_data.get('gender', 0)
                        gender = 'F' if g_val == 1 else ('M' if g_val == 2 else '?')

                        # Check if person already exists in database
                        cur.execute("SELECT peopleid FROM people WHERE surname=%s AND first_name=%s", (sname, fname))
                        exist_p = cur.fetchone()
                        
                        if exist_p:
                            # Use existing person ID
                            pid = exist_p[0]
                        else:
                            # Create new person entry
                            current_max_people_id += 1
                            pid = current_max_people_id
                            
                            # Fetch birth and death dates from TMDB
                            born, died = get_person_dates(p_data['id'])
                            
                            try:
                                cur.execute("""
                                    INSERT INTO people (peopleid, first_name, surname, born, died, gender)
                                    VALUES (%s, %s, %s, %s, %s, %s)
                                """, (pid, fname, sname, born, died, gender))
                            except:
                                conn.rollback()
                                current_max_people_id -= 1
                                continue
                        
                        # Insert movie-person association
                        try:
                            cur.execute("""
                                INSERT INTO credits (movieid, peopleid, credited_as)
                                VALUES (%s, %s, %s)
                                ON CONFLICT DO NOTHING
                            """, (movie_db_id, pid, role))
                        except:
                            conn.rollback()

                count_added += 1
                print(f"  [Success] {title} (with cast and crew)")
                conn.commit()  # Commit changes after each movie

    except Exception as e:
        print(f"Exception occurred: {e}")
    finally:
        cur.close()
        conn.close()
        print("Spanish movies addition completed.")

if __name__ == "__main__":
    process_spanish_movies()