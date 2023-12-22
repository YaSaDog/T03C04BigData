using Microsoft.EntityFrameworkCore;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Net.Http.Headers;
using System.Reflection.Metadata.Ecma335;
using System.Xml.Linq;

namespace T03C04BigData
{
    class Program
    {

        #region Dictionaries
        static ConcurrentDictionary<string, List<Movie>> moviesByTitles = new ConcurrentDictionary<string, List<Movie>>();
        static ConcurrentDictionary<string, HashSet<Movie>> moviesByActor = new ConcurrentDictionary<string, HashSet<Movie>>();
        static ConcurrentDictionary<string, HashSet<Movie>> moviesByDirector = new ConcurrentDictionary<string, HashSet<Movie>>();
        static ConcurrentDictionary<string, HashSet<Movie>> moviesByTags = new ConcurrentDictionary<string, HashSet<Movie>>();

        static ConcurrentDictionary<string, Actor> actorById = new ConcurrentDictionary<string, Actor>();
        static ConcurrentDictionary<string, Director> directorById = new ConcurrentDictionary<string, Director>();
        static ConcurrentDictionary<string, Movie> moviesById = new ConcurrentDictionary<string, Movie>();
        static ConcurrentDictionary<string, Tag> tagById = new ConcurrentDictionary<string, Tag>();

        static ConcurrentDictionary<string, string> matchMovlensImdb = new ConcurrentDictionary<string, string>();
        #endregion

        #region Parsing
        static void ParseData()
        {
            string path = "D://FromDesktop/ml-latest";
            string statsPath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.Desktop),
                "stats.txt");

            Console.WriteLine("Downloading data...");

            Stopwatch sw = new Stopwatch();
            sw.Start();

            //These tasks can run asynchronously
            Task movCodes = ReadMovieCodes(Path.Combine(path, "MovieCodes_IMDB.tsv"));
            Task actDir = ReadActorsDirectors(Path.Combine(path, "ActorsDirectorsNames_IMDB.txt"));
            Task tagCodes = ReadTagCodes(Path.Combine(path, "TagCodes_MovieLens.csv"));

            //Movies' codes are required
            movCodes.Wait();
            Task movLens = Task.Run(() =>
                ReadMovieLens(Path.Combine(path, "links_IMDB_MovieLens.csv")));
            Task ratings = Task.Run(() =>
                ReadRatings(Path.Combine(path, "Ratings_IMDB.tsv")));

            //Movies' and actors' codes are required
            Task actDirCodes = Task.WhenAll(movCodes, actDir).ContinueWith(t =>
                ReadActorsDirectorsCodes(Path.Combine(path, "ActorsDirectorsCodes_IMDB.tsv"))).Unwrap();

            //Tags' codes and Movlens-IMDB are required
            Task tagScores = Task.WhenAll(tagCodes, movLens).ContinueWith(t =>
                ReadTagScores(Path.Combine(path, "TagScores_MovieLens.csv"))).Unwrap();

            Task.WaitAll(ratings, actDirCodes, tagScores);

            sw.Stop();
            Console.WriteLine("Download completed. Time required: " + sw.Elapsed.ToString());
            //WritePerformanceStats(statsPath, sw.Elapsed);
        }

        static Task ReadMovieCodes(string filepath)
        {
            return Task.Factory.StartNew(() =>
            {
                using (StreamReader sr = new StreamReader(filepath))
                {
                    sr.ReadLine();

                    while (!sr.EndOfStream)
                    {
                        string line = sr.ReadLine();

                        int index1 = line.IndexOf('\t');
                        int index2 = line.IndexOf('\t', index1 + 1);
                        int index3 = line.IndexOf('\t', index2 + 1);
                        int index4 = line.IndexOf('\t', index3 + 1);
                        int index5 = line.IndexOf('\t', index4 + 1);

                        string region = line.Substring(index3 + 1, index4 - index3 - 1);
                        string lang = line.Substring(index4 + 1, index5 - index4 - 1);

                        if (region != "RU" && region != "US" && region != "GB" && lang != "ru" && lang != "en")
                        {
                            continue;
                        }

                        string idImdb = line.Substring(0, index1);
                        string title = line.Substring(index2 + 1, index3 - index2 - 1);
                        Movie movie = new Movie(title, idImdb);

                        moviesByTitles.AddOrUpdate(key: title, addValue: new List<Movie> { movie },
                            updateValueFactory: (t, m) => { m.Add(movie); return m; });

                        moviesById[idImdb] = movie;
                    }
                }
                Console.WriteLine("Movie codes downloaded");
            }, TaskCreationOptions.LongRunning);
        }

        static Task ReadActorsDirectors(string filepath)
        {
            return Task.Factory.StartNew(() =>
            {
                using (var sr = new StreamReader(filepath))
                {
                    sr.ReadLine();

                    while (!sr.EndOfStream)
                    {
                        string line = sr.ReadLine();

                        int index1 = line.IndexOf('\t');
                        int index2 = line.IndexOf('\t', index1 + 1);
                        int index3 = line.IndexOf('\t', index2 + 1);
                        int index4 = line.IndexOf('\t', index3 + 1);
                        int index5 = line.IndexOf('\t', index4 + 1);

                        string id = line.Substring(0, index1);
                        string name = line.Substring(index1 + 1, index2 - index1 - 1);
                        string[] professions = line.Substring(index4 + 1, index5 - index4 - 1).Split(",");

                        if ((professions.Contains("actor") || professions.Contains("actress"))
                            && !actorById.ContainsKey(id))
                        {
                            actorById[id] = new Actor(name);
                        }

                        if (professions.Contains("director") && !directorById.ContainsKey(id))
                        {
                            directorById[id] = new Director(name);
                        }
                    }
                }
                Console.WriteLine("Actors/directors downloaded");
            });
        }

        static Task ReadActorsDirectorsCodes(string filepath)
        {
            return Task.Factory.StartNew(() =>
            {
                using (var sr = new StreamReader(filepath))
                {
                    sr.ReadLine();

                    while (!sr.EndOfStream)
                    {
                        string line = sr.ReadLine();

                        int index1 = line.IndexOf('\t');
                        string movieId = line.Substring(0, index1);
                        if (!moviesById.ContainsKey(movieId))
                            continue;

                        int index2 = line.IndexOf('\t', index1 + 1);
                        int index3 = line.IndexOf('\t', index2 + 1);
                        int index4 = line.IndexOf('\t', index3 + 1);
                        int index5 = line.IndexOf('\t', index4 + 1);

                        string humanId = line.Substring(index2 + 1, index3 - index2 - 1);
                        Actor actor = new Actor("unknown");
                        Director director = new Director("unknown");

                        if (directorById.ContainsKey(humanId))
                            director = directorById[humanId];
                        if (actorById.ContainsKey(humanId))
                            actor = actorById[humanId];

                        Movie movie = moviesById[movieId];

                        if (actor.Name == "unknown" && director.Name == "unknown")
                            continue;

                        if (director.Name != "unknown")
                        {
                            movie.Director = director;
                            moviesByDirector.AddOrUpdate(director.Name, new HashSet<Movie> { movie },
                                (d, m) => { m.Add(movie); return m; });
                        }

                        if (actor.Name != "unknown")
                        {
                            movie.Actors.Add(actor);
                            moviesByActor.AddOrUpdate(actor.Name, new HashSet<Movie> { movie },
                                (a, m) => { m.Add(movie); return m; });
                        }
                    }
                }
                Console.WriteLine("Actors/directors codes downloaded");
            });
        }

        static Task ReadRatings(string filepath)
        {
            return Task.Factory.StartNew(() =>
            {
                using (var sr = new StreamReader(filepath))
                {
                    sr.ReadLine();

                    while (!sr.EndOfStream)
                    {
                        string line = sr.ReadLine();

                        int index1 = line.IndexOf('\t');
                        int index2 = line.IndexOf('\t', index1 + 1);
                        int index3 = line.IndexOf('\t', index2 + 1);

                        string id = line.Substring(0, index1);
                        float rating;
                        float.TryParse(line.Substring(index1 + 1, index2 - index1 - 1),
                            NumberStyles.Any, CultureInfo.InvariantCulture, out rating);

                        if (moviesById.ContainsKey(id))
                            moviesById[id].Rating = rating;
                    }
                }
                Console.WriteLine("Ratings downloaded");
            });
        }

        static Task ReadMovieLens(string filepath)
        {
            return Task.Factory.StartNew(() =>
            {
                using (var sr = new StreamReader(filepath))
                {
                    sr.ReadLine();

                    while (!sr.EndOfStream)
                    {
                        string line = sr.ReadLine();

                        int index1 = line.IndexOf(',');
                        int index2 = line.IndexOf(',', index1 + 1);

                        string imdbId = "tt" + line.Substring(index1 + 1, index2 - index1 - 1);

                        if (!moviesById.ContainsKey(imdbId))
                            continue;

                        string movlensId = line.Substring(0, index1);
                        matchMovlensImdb[movlensId] = imdbId;
                    }
                }
                Console.WriteLine("MovieLens codes downloaded");
            });
        }

        static Task ReadTagCodes(string filepath)
        {
            return Task.Factory.StartNew(() =>
            {
                using (var sr = new StreamReader(filepath))
                {
                    sr.ReadLine();

                    while (!sr.EndOfStream)
                    {
                        string line = sr.ReadLine();

                        int index1 = line.IndexOf(',');

                        string tagId = line.Substring(0, index1);
                        string tag = line.Substring(index1 + 1);

                        tagById[tagId] = new Tag(tag);
                    }
                }
                Console.WriteLine("Tag codes downloaded");
            });
        }

        static Task ReadTagScores(string filepath)
        {
            return Task.Factory.StartNew(() =>
            {
                using (var sr = new StreamReader(filepath))
                {
                    sr.ReadLine();

                    while (!sr.EndOfStream)
                    {
                        string line = sr.ReadLine();
                        int index1 = line.IndexOf(',');

                        string movieIdMovlens = line.Substring(0, index1);
                        if (!matchMovlensImdb.ContainsKey(movieIdMovlens))
                            continue;

                        int index2 = line.IndexOf(',', index1 + 1);
                        string tagId = line.Substring(index1 + 1, index2 - index1 - 1);

                        float relevance;
                        float.TryParse(line.Substring(index2 + 1),
                            NumberStyles.Any, CultureInfo.InvariantCulture, out relevance);

                        string movieIdImdb = matchMovlensImdb[movieIdMovlens];
                        Tag tag = tagById[tagId];
                        Movie movie = moviesById[movieIdImdb];

                        if (relevance > 0.5)
                        {
                            if (!movie.Tags.Contains(tag))
                                movie.Tags.Add(tag);

                            moviesByTags.AddOrUpdate(tag.Name, new HashSet<Movie> { movie },
                                (t, m) => { m.Add(movie); return m; });
                        }
                    }
                }
                Console.WriteLine("Tag scores downloaded");
            });
        } 
        #endregion

        static void GetInput()
        {
            var options = new string[]
            {
                "get Movie by title",
                "get Movie by actor",
                "get Movie by director",
                "get Movie by tag",
            };

            int input = 0;
            while (input != -1)
            {
                for (int i = 0; i < options.Length; i++)
                    Console.WriteLine(i + 1 + " - " + options[i]);
                Console.WriteLine("-1 - exit");
                Console.Write("Your choice: ");

                if (!int.TryParse(Console.ReadLine(), out input))
                    continue;

                switch (input)
                {
                    case 1:
                        GetMoviesByTitle();
                        break;
                    case 2:
                        GetMoviesByActor();
                        break;
                    case 3:
                        GetMoviesByDirector();
                        break;
                    case 4:
                        GetMoviesByTag();
                        break;
                    case 0:
                        break;
                }
            }

        }

        #region Get movies
        static void GetMoviesByTitle()
        {
            Console.Write("Name: ");
            string title = Console.ReadLine();

            var movies = GetMoviesByTitle(title);
            PrintMovies(movies);
        }

        static HashSet<Movie> GetMoviesByTitle(string title)
        {
            using (var db = new ApplicationContext())
            {
                string lowTitle = title.ToLower();
                var movies = db.Movies
                    .Where(m => m.Title == title)
                    .Include(m => m.Actors)
                    .Include(m => m.Director)
                    .Include(m => m.Tags)
                    .Include(m => m.SimilarMovies)
                        .ThenInclude(m => m.Actors)
                    .Include(m => m.SimilarMovies)
                        .ThenInclude(m => m.Director)
                    .Include(m => m.SimilarMovies)
                        .ThenInclude(m => m.Tags)
                    .AsNoTracking()
                    .AsSplitQuery()
                    .Where(m => m.Title.ToLower().Contains(lowTitle))
                    .ToHashSet();

                return movies;
            }
        }

        static void GetMoviesByActor()
        {
            Console.Write("Actor: ");
            string actorName = Console.ReadLine();

            var movies = GetMoviesByActor(actorName);
            PrintMovies(movies);
        }

        static HashSet<Movie> GetMoviesByActor(string actorName)
        {
            using (var db = new ApplicationContext())
            {
                string name = actorName.ToLower();

                var movies = db.Actors
                    .Where(a => a.Name.ToLower() == name)
                    .Include(a => a.Movies)
                    .SelectMany(a => a.Movies)
                    .OrderByDescending(m => m.Rating)
                    .ToHashSet();

                return movies;
            }
        }

        static void GetMoviesByDirector()
        {
            Console.Write("Director: ");
            string directorName = Console.ReadLine();

            var movies = GetMoviesByDirector(directorName);
            PrintMovies(movies);
        }

        static HashSet<Movie> GetMoviesByDirector(string directorName)
        {
            using (var db = new ApplicationContext())
            {
                string name = directorName.ToLower();
                var movies = db.Directors
                    .Where(d => d.Name.ToLower() == name)
                    .Include(d => d.Movies)
                    .SelectMany(d => d.Movies)
                    .OrderByDescending(m => m.Rating)
                    .ToHashSet();

                return movies;
            }
        }

        static void GetMoviesByTag()
        {
            Console.Write("Tag: ");
            string tagName = Console.ReadLine();

            var movies = GetMoviesByTag(tagName);
            PrintMovies(movies);
        }

        static HashSet<Movie> GetMoviesByTag(string tagName)
        {
            using (var db = new ApplicationContext())
            {
                string name = tagName.ToLower();
                var movies = db.Tags
                    .Where(t => t.Name.ToLower() == name)
                    .Include(t => t.Movies)
                    .SelectMany(t => t.Movies)
                    .OrderByDescending(t => t.Rating)
                    .ToHashSet();

                return movies;
            }
        } 
        #endregion

        static void PrintMovies(ICollection<Movie> movies)
        {
            Console.WriteLine();
            foreach (var m in movies)
            {
                Console.WriteLine(m.ToString());
            }
            Console.WriteLine($"{movies.Count} movies found\n");
        }

        static void WritePerformanceStats(string filepath, TimeSpan time)
        {
            using (StreamWriter writer = new StreamWriter(filepath))
            {
                writer.WriteLine(time.TotalSeconds.ToString());
            }
        }

        static void WriteToDB()
        {
            Stopwatch sw = new Stopwatch();

            Console.WriteLine("Writing to DB...");
            sw.Start();
            using (ApplicationContext db = new ApplicationContext())
            {
                db.Database.EnsureDeleted();
                db.Database.EnsureCreated();

                db.Actors.BulkMerge(actorById.Values, options => options.IncludeGraph = true);
                db.Directors.BulkMerge(directorById.Values, options => options.IncludeGraph = true);
                db.Tags.BulkMerge(tagById.Values, options => options.IncludeGraph = true);

                db.Movies.BulkInsert(moviesById.Values, options => options.IncludeGraph = true);
            }

            sw.Stop();
            Console.WriteLine("DB created. Time required: " + sw.Elapsed.ToString());
        }

        static void TestCoef()
        {
            using (var db = new ApplicationContext())
            {
                var m1 = GetMoviesByTitle("LaLa Land").FirstOrDefault();
                Console.WriteLine(m1.ToString());

                var m2 = GetMoviesByTitle("Deadpool").FirstOrDefault();
                Console.WriteLine(m2.ToString());

                Console.WriteLine("m1 <- m1: " + m1.GetSimilarityCoefficient(m1));
                Console.WriteLine("m2 <- m2: " + m2.GetSimilarityCoefficient(m2));
                Console.WriteLine("m1 <- m2: " + m1.GetSimilarityCoefficient(m2));
                Console.WriteLine("m2 <- m1: " + m2.GetSimilarityCoefficient(m1));
            }
        }

        static void TestSimilar()
        {
            using (var db = new ApplicationContext())
            {
                var m1 = GetMoviesByTitle("Terminator").FirstOrDefault();
                Console.WriteLine(m1.ToString());

                //PrintMovies(m1.SimilarMovies);
            }
        }

        static void GetSimilarMovies()
        {
            Console.WriteLine("Start similar");
            var sw = new Stopwatch();
            sw.Start();

            Parallel.ForEach(moviesById.Values, m =>
            {
                m.GetSimilar(moviesByActor, moviesByDirector, moviesByTags);
            });

            sw.Stop();
            Console.WriteLine("End similar. Time: " + sw.Elapsed.ToString());
        }

        static void Main(string[] args)
        {
            //ParseData();
            //GetSimilarMovies();
            //WriteToDB();

            GetInput();
        }
    }
}