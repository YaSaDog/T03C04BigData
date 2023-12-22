using Microsoft.EntityFrameworkCore;

namespace T03C04BigData
{
    public class ApplicationContext : DbContext
    {
        public DbSet<Movie> Movies => Set<Movie>();
        public DbSet<Director> Directors => Set<Director>();
        public DbSet<Actor> Actors => Set<Actor>();
        public DbSet<Tag> Tags => Set<Tag>();

        public ApplicationContext()
        {
        }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseSqlite("Data Source=mynew.db");
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);

            modelBuilder.Entity<Movie>()
                .HasMany(m => m.SimilarMovies)
                .WithMany(m => m.SimilarTo);
        }
    }
}
