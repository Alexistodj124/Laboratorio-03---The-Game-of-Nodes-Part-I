from neo4j import GraphDatabase

# Conexión a Neo4j (ajusta la URI, usuario y contraseña)
URI = "neo4j+s://16ada6a9.databases.neo4j.io"
USER = "neo4j"
PASSWORD = "N-5lDXtG3fYys1Q_4qPIUZ2LKL9pBjii17swEWSCjd0"

class MovieGraph:
    def _init_(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def run_query(self, query, parameters=None):
        with self.driver.session() as session:
            session.run(query, parameters or {})

    def create_user(self, name, userId):
        query = "CREATE (:User {name: $name, userId: $userId})"
        self.run_query(query, {"name": name, "userId": userId})

    def create_movie(self, title, movieId, year, imdbRating, plot):
        query = """
        CREATE (:Movie {title: $title, movieId: $movieId, year: $year, imdbRating: $imdbRating, plot: $plot})
        """
        self.run_query(query, {"title": title, "movieId": movieId, "year": year, "imdbRating": imdbRating, "plot": plot})

    def create_person(self, name, tmdbId, role):
        query = "CREATE (:Person {name: $name, tmdbId: $tmdbId, role: $role})"
        self.run_query(query, {"name": name, "tmdbId": tmdbId, "role": role})

    def create_genre(self, name):
        query = "CREATE (:Genre {name: $name})"
        self.run_query(query, {"name": name})

    def create_rating(self, userId, movieId, rating, timestamp):
        query = """
        MATCH (u:User {userId: $userId}), (m:Movie {movieId: $movieId})
        CREATE (u)-[:RATED {rating: $rating, timestamp: $timestamp}]->(m)
        """
        self.run_query(query, {"userId": userId, "movieId": movieId, "rating": rating, "timestamp": timestamp})

    def create_acting_relationship(self, personName, movieId, role):
        query = """
        MATCH (p:Person {name: $personName}), (m:Movie {movieId: $movieId})
        CREATE (p)-[:ACTED_IN {role: $role}]->(m)
        """
        self.run_query(query, {"personName": personName, "movieId": movieId, "role": role})

    def create_directing_relationship(self, personName, movieId):
        query = """
        MATCH (p:Person {name: $personName}), (m:Movie {movieId: $movieId})
        CREATE (p)-[:DIRECTED]->(m)
        """
        self.run_query(query, {"personName": personName, "movieId": movieId})

    def create_movie_genre_relationship(self, movieId, genreName):
        query = """
        MATCH (m:Movie {movieId: $movieId}), (g:Genre {name: $genreName})
        CREATE (m)-[:IN_GENRE]->(g)
        """
        self.run_query(query, {"movieId": movieId, "genreName": genreName})

# Crear la instancia del grafo
graph = MovieGraph(URI, USER, PASSWORD)

# Agregar 5 usuarios
users = [
    ("Alice", "U1"),
    ("Bob", "U2"),
    ("Charlie", "U3"),
    ("David", "U4"),
    ("Emma", "U5")
]

for name, userId in users:
    graph.create_user(name, userId)

# Agregar 4 películas
movies = [
    ("Inception", 1, 2010, 8.8, "A thief enters dreams to steal secrets."),
    ("The Matrix", 2, 1999, 8.7, "A hacker discovers the real world is a simulation."),
    ("Interstellar", 3, 2014, 8.6, "A space mission to find a new home for humanity."),
    ("The Dark Knight", 4, 2008, 9.0, "Batman fights the Joker in Gotham.")
]

for title, movieId, year, imdbRating, plot in movies:
    graph.create_movie(title, movieId, year, imdbRating, plot)

# Agregar 4 actores/directores
people = [
    ("Leonardo DiCaprio", 101, "Actor"),
    ("Keanu Reeves", 102, "Actor"),
    ("Christopher Nolan", 201, "Director"),
    ("Lana Wachowski", 202, "Director")
]

for name, tmdbId, role in people:
    graph.create_person(name, tmdbId, role)

# Agregar 3 géneros
genres = ["Sci-Fi", "Action", "Drama"]

for genre in genres:
    graph.create_genre(genre)

# Relacionar usuarios con películas (RATED)
ratings = [
    ("U1", 1, 5, 1610000000), ("U1", 2, 4, 1610005000),
    ("U2", 2, 5, 1610010000), ("U2", 3, 3, 1610015000),
    ("U3", 1, 4, 1610020000), ("U3", 4, 5, 1610025000),
    ("U4", 3, 5, 1610030000), ("U4", 4, 4, 1610035000),
    ("U5", 1, 3, 1610040000), ("U5", 2, 5, 1610045000)
]

for userId, movieId, rating, timestamp in ratings:
    graph.create_rating(userId, movieId, rating, timestamp)

# Relacionar actores con películas (ACTED_IN)
acting = [
    ("Leonardo DiCaprio", 1, "Cobb"),
    ("Keanu Reeves", 2, "Neo")
]

for personName, movieId, role in acting:
    graph.create_acting_relationship(personName, movieId, role)

# Relacionar directores con películas (DIRECTED)
directing = [
    ("Christopher Nolan", 1),
    ("Christopher Nolan", 3),
    ("Lana Wachowski", 2)
]

for personName, movieId in directing:
    graph.create_directing_relationship(personName, movieId)

# Relacionar películas con géneros (IN_GENRE)
movie_genres = [
    (1, "Sci-Fi"), (2, "Sci-Fi"), (2, "Action"),
    (3, "Drama"), (4, "Action"), (4, "Drama")
]

for movieId, genreName in movie_genres:
    graph.create_movie_genre_relationship(movieId, genreName)

# Cerrar la conexión
graph.close()
