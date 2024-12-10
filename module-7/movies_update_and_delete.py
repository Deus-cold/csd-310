import mysql.connector
from mysql.connector import Error

def connect_to_movies_db():
    try:
        connection = mysql.connector.connect(
             user= "root",
             password= "root123",
             host= "127.0.0.1",
             database= "movies",
             raise_on_warnings= True)
        
        if connection.is_connected():
            print("Connected to the 'movies' database successfully!\n")
            return connection
    except Error as e:
        print(f"Error connecting to MySQL {e}")
        return None




def show_films(cursor, title):

    # Method to execute an inner join on all tables,
    # Iterate over the dataset and output the results to the terminal window.
    
    # Inner join query
    cursor.execute("SELECT film_name AS Name, film_director AS Director, genre_name AS Genre, studio_name AS 'Studio Name' from film INNER JOIN genre ON film.genre_id=genre.genre_id INNER JOIN studio ON film.studio_id=studio.studio_id")
     
    # get the results from the cursor object
    films = cursor.fetchall()

    print("\n -- {} --".format(title))

    # Iterate over the film data set and display the results
    for film in films:
        print("Film Name: {}\n Director: {}\nGenre Name ID: {}\nStudio Name: {}\n".format(film[0], film[1], film[2], film[3]))



def main():
    """Main logic for updates & deletes records in movie database """

    connection = connect_to_movies_db()

    if connection:
        try:
            cursor = connection.cursor()

            # Initial films
            show_films(cursor, "DISPLAYING FILMS")
        
            # Insert new film
            new_film = """
            INSERT INTO film (film_name, film_releaseDate, film_runtime, film_director, studio_id, genre_id)
            VALUES ('Inception', '2010', '148', 'Christopher Nolan',
                     (SELECT studio_id FROM studio WHERE studio_name = 'Universal Pictures'),
                     (SELECT genre_id FROM genre WHERE genre_name = 'SciFi'));
            """
            

            cursor.execute(new_film)
            connection.commit()
            show_films(cursor, "DISPLAYING FILMS AFTER INSERT")

            # Update the genre of the film Alien to Horror
            update_genre = """
            UPDATE film
            SET genre_id = (SELECT genre_id FROM genre WHERE genre_name = 'Horror')
            WHERE film_name = 'Alien';
            """
            cursor.execute(update_genre)
            connection.commit()
            show_films(cursor, "DISPLAYING FILMS AFTER UPDATE")

            # Delete the movie Gladiator
            delete_film = """
            DELETE FROM film
            WHERE film_name = 'Gladiator';
            """
            cursor.execute(delete_film)
            connection.commit()
            show_films(cursor, 'DISPLAYING FILMS AFTER DELETE')

        except Error as e:
            print(f"An error occurred: {e}")

        finally:
            connection.close()
            print("Connection closed.")

if __name__ == "__main__":
    main()
