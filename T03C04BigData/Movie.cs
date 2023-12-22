using Microsoft.EntityFrameworkCore;
using System.Collections.Concurrent;
using System.ComponentModel.DataAnnotations.Schema;

namespace T03C04BigData
{
    public class Movie : IEquatable<Movie>
    {
        public int Id { get; set; }
        public string Title { get; set; }
        public float Rating { get; set; }
        public string IdIMDB { get; set; }

        public int? DirectorId { get; set; }
        public Director Director { get; set; } = new();
        public HashSet<Actor> Actors { get; set; } = new();
        public HashSet<Tag> Tags { get; set; } = new();

        public HashSet<Movie> SimilarTo { get; set; } = new();
        public HashSet<Movie> SimilarMovies { get; set; } = new();

        public bool Equals(Movie? obj)
        {
            if (obj is Movie m)
                return Title == m.Title && Rating == m.Rating &&
                    Director.Name == m.Director.Name;

            return false;
        }

        /// <summary>
        /// Get top-10 similar movies for this one
        /// </summary>
        public void GetSimilar(ConcurrentDictionary<string, HashSet<Movie>> moviesByActor,
            ConcurrentDictionary<string, HashSet<Movie>> moviesByDirector,
            ConcurrentDictionary<string, HashSet<Movie>> moviesByTags)
        {
            IEnumerable<Movie> movies = new HashSet<Movie>();

            foreach (var a in Actors)
                if (a != null && a.Name != null)
                    movies = movies.Union(moviesByActor[a.Name]);
            
            if (Director != null && Director.Name != null) 
                movies = movies.Union(moviesByDirector[Director.Name]);

            foreach (var t in Tags)
                if (t!= null && t.Name != null)
                    movies = movies.Union(moviesByTags[t.Name]);

            movies = movies.Except(new HashSet<Movie>() { this });

            SimilarMovies = movies.OrderByDescending(GetSimilarityCoefficient).Take(10).ToHashSet();
        }

        /// <summary>
        /// Get similarity coefficient for the chosen movie and this one
        /// </summary>
        /// <returns>Coefficient in range [0; 1]</returns>
        public float GetSimilarityCoefficient(Movie movie)
        {
            int actorsCount = Actors.Intersect(movie.Actors).Count();
            int tagsCount = Tags.Intersect(movie.Tags).Count();
            int dirCount = Director.Name == movie.Director.Name ? 1 : 0;

            int intersectCount = actorsCount + tagsCount + dirCount;

            float intersectionCoef = intersectCount / 
                (2.0f * (Math.Min(Actors.Count(), movie.Actors.Count()) + 
                Math.Min(Tags.Count(), movie.Tags.Count()) + 1));

            float ratingCoef = movie.Rating * 0.05f;

            return ratingCoef + intersectionCoef;
        }

        public Movie() 
        { }

        public Movie(string title)
        {
            Title = title;
        }

        public Movie(string title, string idIMDB) : this(title)
        {
            IdIMDB = idIMDB;
        }

        public override string ToString()
        {
            string res = "========\n";

            res += "Title: " + Title;
            res += "\nRating: " + Rating;
            res += "\nDirector: " + Director;

            res += "\nActors:";
            int i = 0;
            foreach (var actor in Actors)
            {
                i++;
                res += "  " + i + ") " + actor;
            }

            res += "\nTags:";
            int j = 0;
            foreach (var tag in Tags)
            {
                i++;
                res += "  " + i + ") " + tag;
            }

            res += "\n========";
            return res;
        }
    }
}